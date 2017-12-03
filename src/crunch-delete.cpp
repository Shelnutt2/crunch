/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include "crunchrowlocation.capnp.h"
#include <capnp/serialize.h>
#include "crunch.hpp"
#include "utils.hpp"
#include "crunch-txn.hpp"
#include <stdio.h>
#include <iostream>

int crunch::readDeletesIntoMap(int deleteFileDescriptor) {
  long size = lseek(deleteFileDescriptor, 0, SEEK_END); // seek to end of file
  lseek(deleteFileDescriptor, 0, SEEK_SET); // seek back to beginning of file
  if (size > 0) {
    while (lseek(deleteFileDescriptor, 0, SEEK_CUR) != size) {
      try {
        capnp::StreamFdMessageReader message(deleteFileDescriptor);
        CrunchRowLocation::Reader deletedRow = message.getRoot<CrunchRowLocation>();

        auto fileMap = deleteMap.find(deletedRow.getFileName());
        if (fileMap != deleteMap.end()) {
          fileMap->second->emplace(deletedRow.getRowStartLocation(), deletedRow);
        } else { // New file!
          std::shared_ptr<std::unordered_map<uint64_t, CrunchRowLocation::Reader>> newFile =
            std::shared_ptr<std::unordered_map<uint64_t, CrunchRowLocation::Reader>>(
              new std::unordered_map<uint64_t, CrunchRowLocation::Reader>());
          newFile->emplace(deletedRow.getRowStartLocation(), deletedRow);
          deleteMap.emplace(deletedRow.getFileName(), newFile);
        }
      } catch (kj::Exception e) {
        if (e.getDescription() != "expected n >= minBytes; Premature EOF") {
          std::cerr << "exception: " << e.getFile() << ", line: "
                    << e.getLine() << ", type: " << (int) e.getType()
                    << ", e.what(): " << e.getDescription().cStr() << std::endl;
          return -1;
        } else {
          break;
        }
      } catch (std::exception e) {
        std::cerr << "crunch: Error reading delete file: " << e.what() << std::endl;
        return -1;
      }
    }
  }
  return 0;
}

bool crunch::checkForDeletedRow(std::string fileName, uint64_t rowStartLocation) {
  auto fileNameMap = deleteMap.find(fileName);
  if(fileNameMap == deleteMap.end())
    return false;

  auto record = fileNameMap->second->find(rowStartLocation);
  if(record == fileNameMap->second->end())
    return false;

  return true;
}

void crunch::markRowAsDeleted(std::string fileName, uint64_t rowStartLocation, uint64_t rowEndLocation) {

  crunchTxn *txn= (crunchTxn *)thd_get_ha_data(ha_thd(), crunch_hton);

  //DBUG_PRINT("debug", ("Transaction is running: %d, uuid: %s", txn->inProgress, txn->uuid.str().c_str()));
  if(!is_fd_valid(txn->transactionDeleteFileDescriptor)) {
    //std::cerr << "File descriptor ("  << ") not valid, opening:" << txn->transactionDeleteFile << std::endl;
    txn->transactionDeleteFileDescriptor = my_open(txn->transactionDeleteFile.c_str(), O_RDWR | O_CREAT, 0);
  }

  capnp::MallocMessageBuilder deleteRow;
  CrunchRowLocation::Builder builder = deleteRow.initRoot<CrunchRowLocation>();
  builder.setFileName(fileName);
  builder.setRowEndLocation(rowEndLocation);
  builder.setRowStartLocation(rowStartLocation);

  if(!is_fd_valid(txn->transactionDeleteFileDescriptor)) {
    txn->transactionDeleteFileDescriptor = my_open(deleteFile.c_str(), O_RDWR | O_CREAT, 0);
  }

  // Set the fileDescriptor to the end of the file
  lseek(txn->transactionDeleteFileDescriptor , 0, SEEK_END);
  //Write message to file
  capnp::writeMessageToFd(txn->transactionDeleteFileDescriptor , deleteRow);
}
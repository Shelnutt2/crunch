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
#include <libgen.h>

/**
 * Takes a file descriptor and reads delete messages into a map
 * @param deleteFileDescriptor
 * @return
 */
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
          std::cerr << "exception: " << e.getFile() << ", line: " << __FILE__ << ":" << __LINE__ <<", exception_line: "
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

/**
 * Checks for if a row is deleted or not
 * @param fileName
 * @param rowStartLocation
 * @return Returns true if row is deleted
 */
bool crunch::checkForDeletedRow(std::string fileName, uint64_t rowStartLocation) {
  char* buff = new char[fileName.size()+1];
  strcpy(buff, fileName.c_str());
  std::string deleteFileName = basename(buff);
  auto fileNameMap = deleteMap.find(deleteFileName);
  if(fileNameMap == deleteMap.end())
    return false;

  auto record = fileNameMap->second->find(rowStartLocation);
  if(record == fileNameMap->second->end())
    return false;

  return true;
}

/**
 * Marks a row for deletion
 * @param fileName
 * @param rowStartLocation
 * @param rowEndLocation
 */
void crunch::markRowAsDeleted(std::string fileName, uint64_t rowStartLocation, uint64_t rowEndLocation) {

  crunchTxn *txn= (crunchTxn *)thd_get_ha_data(ha_thd(), crunch_hton);

  capnp::MallocMessageBuilder deleteRow;
  CrunchRowLocation::Builder builder = deleteRow.initRoot<CrunchRowLocation>();
  builder.setFileName(fileName);
  builder.setRowEndLocation(rowEndLocation);
  builder.setRowStartLocation(rowStartLocation);

  // Set the fileDescriptor to the end of the file
  lseek(txn->getTransactionDeleteFileDescriptor(this->name), 0, SEEK_END);
  //Write message to file
  try {
    capnp::writeMessageToFd(txn->getTransactionDeleteFileDescriptor(this->name), deleteRow);
  } catch (kj::Exception e) {
    if (e.getDescription() != "expected n >= minBytes; Premature EOF") {
      std::cerr << "exception: " << e.getFile() << ", line: " << __FILE__ << ":" << __LINE__ <<", exception_line: "
                << e.getLine() << ", type: " << (int) e.getType()
                << ", e.what(): " << e.getDescription().cStr() << std::endl;
      txn->isTxFailed = true;
    }
  } catch (std::exception e) {
    std::cerr << "crunch: Error reading delete file: " << e.what() << std::endl;
    txn->isTxFailed = true;
  }
}
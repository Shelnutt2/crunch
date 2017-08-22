/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include "crunchdeleterow.capnp.h"
#include <capnp/serialize.h>
#include "crunch.hpp"
#include <stdio.h>
#include <iostream>

int crunch::readDeletesIntoMap(FILE* deleteFilePointer) {
  if(deleteFilePointer != NULL) {
    fseek(deleteFilePointer, 0, SEEK_END); // seek to end of file
    long size = ftell(deleteFilePointer); // get current file pointer
    fseek(deleteFilePointer, 0, SEEK_SET); // seek back to beginning of file
    if(size) {
      while (!feof(deleteFilePointer)) {
        try {
        capnp::StreamFdMessageReader message(fileno(deleteFilePointer));
        CrunchDeleteRow::Reader deletedRow = message.getRoot<CrunchDeleteRow>();

        auto fileMap = deleteMap.find(deletedRow.getFileName());
        if (fileMap != deleteMap.end()) {
          fileMap->second->emplace(deletedRow.getRowStartLocation(), deletedRow);
        } else { // New file!
          std::shared_ptr<std::unordered_map<uint64_t, CrunchDeleteRow::Reader>> newFile = std::shared_ptr<std::unordered_map<uint64_t, CrunchDeleteRow::Reader>>(
            new std::unordered_map<uint64_t, CrunchDeleteRow::Reader>());
          newFile->emplace(deletedRow.getRowStartLocation(), deletedRow);
          deleteMap.emplace(deletedRow.getFileName(), newFile);
        }
        } catch (std::exception e) {
          std::cerr << "crunch: Error reading delete file: " << e.what() << std::endl;
          return -1;
        }
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

  capnp::MallocMessageBuilder deleteRow;
  CrunchDeleteRow::Builder builder = deleteRow.initRoot<CrunchDeleteRow>();
  builder.setFileName(fileName);
  builder.setRowEndLocation(rowEndLocation);
  builder.setRowStartLocation(rowStartLocation);

  auto fileMap = deleteMap.find(fileName);
  if(fileMap != deleteMap.end()) {
    fileMap->second->emplace(rowStartLocation, builder);
  } else { // New file!
    std::shared_ptr<std::unordered_map<uint64_t,CrunchDeleteRow::Reader>> newFile =  std::shared_ptr<std::unordered_map<uint64_t,CrunchDeleteRow::Reader>>(new std::unordered_map<uint64_t,CrunchDeleteRow::Reader>());
    newFile->emplace(rowStartLocation, builder);
    deleteMap.emplace(fileName, newFile);
  }

  // Set the fileDescriptor to the end of the file
  lseek(fileno(deleteFilePointer), 0, SEEK_END);
  //Write message to file
  capnp::writeMessageToFd(fileno(deleteFilePointer), deleteRow);
}
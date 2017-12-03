//
// Created by Seth Shelnutt on 11/10/17.
//

#include <iostream>
#include "crunch-txn.hpp"
#include "utils.hpp"

crunchTxn::crunchTxn(std::string baseDirectory, std::string transactionDirectory) {
  this->baseDirectory = baseDirectory;
  this->transactionDirectory = transactionDirectory;
  this->isTxFailed = false;
}

crunchTxn::~crunchTxn() {
  if(is_fd_valid(transactionDataFileDescriptor))
    my_close(transactionDataFileDescriptor, 0);
  if(is_fd_valid(transactionDeleteFileDescriptor))
    my_close(transactionDeleteFileDescriptor, 0);
}

int crunchTxn::begin() {

  char name_buff[FN_REFLEN];
  int ret = 0;

  this->startTimeMilliSeconds = std::chrono::duration_cast<std::chrono::nanoseconds >(std::chrono::high_resolution_clock::now().time_since_epoch());

  uuid = sole::uuid4();
  this->baseFileName = std::to_string(this->startTimeMilliSeconds.count()) + "-" + uuid.str();

  this->transactionDataFile = fn_format(name_buff, baseFileName.c_str(), transactionDirectory.c_str(), TABLE_DATA_EXTENSION,
            MY_REPLACE_EXT|MY_UNPACK_FILENAME);
  this->transactionDataFileDescriptor = my_create(this->transactionDataFile.c_str(),0,
            O_RDWR | O_TRUNC,MYF(MY_WME));

  this->transactionDeleteFile = fn_format(name_buff, baseFileName.c_str(), transactionDirectory.c_str(), TABLE_DELETE_EXTENSION,
                                        MY_REPLACE_EXT|MY_UNPACK_FILENAME);
  this->transactionDeleteFileDescriptor = my_create(this->transactionDeleteFile.c_str(),0,
                                                  O_RDWR | O_TRUNC,MYF(MY_WME));

  this->inProgress = true;
  return ret;
}

int crunchTxn::commit_or_rollback() {
  int res;
  if (this->isTxFailed) {
    this->rollback();
    res = false;
  } else {
    res = this->commit();
  }
  return res;
}

int crunchTxn::commit() {
  int res = 0;
  char name_buff[FN_REFLEN];

  if(is_fd_valid(transactionDataFileDescriptor))
    res = my_close(transactionDataFileDescriptor, 0);
  if(getFilesize(transactionDataFile.c_str()) > 0) {
    my_rename(transactionDataFile.c_str(), fn_format(name_buff, baseFileName.c_str(), baseDirectory.c_str(),
                                                     TABLE_DATA_EXTENSION,  MY_REPLACE_EXT|MY_UNPACK_FILENAME), 0);
  } else {
    my_delete(transactionDataFile.c_str(), 0);
  }


  if(is_fd_valid(transactionDeleteFileDescriptor))
    res = my_close(transactionDeleteFileDescriptor, 0);
  if(getFilesize(transactionDeleteFile.c_str()) > 0) {
    my_rename(transactionDeleteFile.c_str(), fn_format(name_buff, baseFileName.c_str(), baseDirectory.c_str(),
                                                     TABLE_DELETE_EXTENSION,  MY_REPLACE_EXT|MY_UNPACK_FILENAME), 0);
  } else {
    my_delete(transactionDeleteFile.c_str(), 0);
  }

  this->inProgress = false;
  return res;
}

int crunchTxn::rollback() {
  int res = 0;
  if(is_fd_valid(transactionDataFileDescriptor))
    res = my_close(transactionDataFileDescriptor, 0);
  my_delete(transactionDataFile.c_str(), 0);
  if(is_fd_valid(transactionDeleteFileDescriptor))
    res = my_close(transactionDeleteFileDescriptor, 0);
  my_delete(transactionDeleteFile.c_str(), 0);
  this->inProgress = false;
  return res;
}

//
// Created by Seth Shelnutt on 11/10/17.
//

#include <iostream>
#include "crunch-txn.hpp"
#include "utils.hpp"

/**
 * Create new transaction
 * @param name
 * @param dataDirectory if the dataDirectory is omitted then a new directory is created based on the trx id
 * @param transactionDirectory
 * @param schemaVersion
 */
crunchTxn::crunchTxn(std::string name, std::string dataDirectory, std::string transactionDirectory,
                     uint64_t schemaVersion) {
  filesForTransaction *file = new filesForTransaction{};
  file->tableName = name;
  if (!dataDirectory.empty())
    file->dataDirectory = dataDirectory;
  else {
    file->dataDirectory = name + "/" + std::to_string(this->startTimeNanoSeconds) + "-" + uuid.str();
    createDirectory(file->dataDirectory);
  }
  file->transactionDirectory = transactionDirectory;
  file->schemaVersion = schemaVersion;
  file->dataExtension = ("." + std::to_string(schemaVersion) + TABLE_DATA_EXTENSION);

  this->tables[name] = file;
  this->isTxFailed = false;
  this->tablesInUse = 1;
}

/**
 * Destructor
 */
crunchTxn::~crunchTxn() {
  for (auto table : this->tables) {
    filesForTransaction *file = table.second;
    if (isFdValid(file->transactionDataFileDescriptor))
      my_close(file->transactionDataFileDescriptor, 0);
    if (isFdValid(file->transactionDeleteFileDescriptor))
      my_close(file->transactionDeleteFileDescriptor, 0);

    delete file;
  }
}

/**
 * Add a new table to an existing transaction
 * @param name
 * @param dataDirectory if the dataDirectory is omitted then a new directory is created based on the trx id
 * @param transactionDirectory
 * @param schemaVersion
 * @return
 */
int crunchTxn::registerNewTable(std::string name, std::string dataDirectory, std::string transactionDirectory,
                                uint64_t schemaVersion) {
  // If table already exists don't re-register it
  if (this->tables.find(name) != this->tables.end()) {
    return 0;
  }
  char name_buff[FN_REFLEN];

  // Create new files struct for new table
  filesForTransaction *file = new filesForTransaction{};
  file->tableName = name;
  if (!dataDirectory.empty())
    file->dataDirectory = dataDirectory;
  else {
    file->dataDirectory = name + "/" + std::to_string(this->startTimeNanoSeconds) + "-" + uuid.str();
    createDirectory(file->dataDirectory);
  }
  file->transactionDirectory = transactionDirectory;
  file->schemaVersion = schemaVersion;
  file->dataExtension = ("." + std::to_string(schemaVersion) + TABLE_DATA_EXTENSION);

  file->baseFileName = std::to_string(this->startTimeNanoSeconds) + "-" + uuid.str();

  file->transactionDataFile = fn_format(name_buff, file->baseFileName.c_str(), file->transactionDirectory.c_str(),
                                        file->dataExtension.c_str(),
                                        MY_REPLACE_EXT | MY_UNPACK_FILENAME);
  file->transactionDataFileDescriptor = my_create(file->transactionDataFile.c_str(), 0,
                                                  O_RDWR | O_TRUNC, MYF(MY_WME));

  file->transactionDeleteFile = fn_format(name_buff, file->baseFileName.c_str(), file->transactionDirectory.c_str(),
                                          TABLE_DELETE_EXTENSION,
                                          MY_REPLACE_EXT | MY_UNPACK_FILENAME);
  file->transactionDeleteFileDescriptor = my_create(file->transactionDeleteFile.c_str(), 0,
                                                    O_RDWR | O_TRUNC, MYF(MY_WME));

  this->tables[name] = file;
  this->tablesInUse++;

  return 0;
}

/**
 * Start a transaction
 * @return
 */
int crunchTxn::begin() {

  char name_buff[FN_REFLEN];
  int ret = 0;

  this->startTimeNanoSeconds = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::high_resolution_clock::now().time_since_epoch()).count();

  uuid = sole::uuid4();
  //For each table we need to create files in disk to hold transaction data
  for (auto table : this->tables) {
    filesForTransaction *file = table.second;
    file->baseFileName = std::to_string(this->startTimeNanoSeconds) + "-" + uuid.str();

    file->transactionDataFile = fn_format(name_buff, file->baseFileName.c_str(), file->transactionDirectory.c_str(),
                                          file->dataExtension.c_str(),
                                          MY_REPLACE_EXT | MY_UNPACK_FILENAME);

    if (!isFdValid(file->transactionDataFileDescriptor)) {
      file->transactionDataFileDescriptor = my_create(file->transactionDataFile.c_str(), 0,
                                                      O_RDWR | O_TRUNC, MYF(MY_WME));
    }

    file->transactionDeleteFile = fn_format(name_buff, file->baseFileName.c_str(), file->transactionDirectory.c_str(),
                                            TABLE_DELETE_EXTENSION,
                                            MY_REPLACE_EXT | MY_UNPACK_FILENAME);

    if (!isFdValid(file->transactionDeleteFileDescriptor)) {
      file->transactionDeleteFileDescriptor = my_create(file->transactionDeleteFile.c_str(), 0,
                                                        O_RDWR | O_TRUNC, MYF(MY_WME));
    }
  }
  this->inProgress = true;
  return ret;
}

/**
 * Attempts to commit the transaction, rolls back on any errors.
 * @return
 */
int crunchTxn::commitOrRollback() {
  int res;
  if (this->isTxFailed) {
    this->rollback();
    res = -111;
  } else {
    res = this->commit();
  }
  return res;
}

/**
 * Commits a transaction, one file at a time
 * @return
 */
int crunchTxn::commit() {
  int res = 0;
  char name_buff[FN_REFLEN];

  for (auto table : this->tables) {
    filesForTransaction *file = table.second;
    if (isFdValid(file->transactionDataFileDescriptor)) {
      res = my_close(file->transactionDataFileDescriptor, 0);
      if (!res)
        file->transactionDataFileDescriptor = 0;
    }
    if (getFilesize(file->transactionDataFile.c_str()) > 0) {
      std::string renameFile = fn_format(name_buff, file->baseFileName.c_str(), file->dataDirectory.c_str(),
                                         file->dataExtension.c_str(),
                                         MY_REPLACE_EXT | MY_UNPACK_FILENAME);
      res = my_rename(file->transactionDataFile.c_str(), renameFile.c_str(), 0);
      // If rename was not successful return and rollback
      if (res)
        return res;
      // Set new filename in case another table rollback fails and we need to roll this back
      file->transactionDataFile = renameFile;
    } else {
      if (checkFileExists(file->transactionDataFile.c_str()))
        my_delete(file->transactionDataFile.c_str(), 0);
    }


    if (isFdValid(file->transactionDeleteFileDescriptor)) {
      res = my_close(file->transactionDeleteFileDescriptor, 0);
      if (!res)
        file->transactionDeleteFileDescriptor = 0;
    }
    if (getFilesize(file->transactionDeleteFile.c_str()) > 0) {
      std::string renameFile = fn_format(name_buff, file->baseFileName.c_str(), file->dataDirectory.c_str(),
                                         TABLE_DELETE_EXTENSION, MY_REPLACE_EXT | MY_UNPACK_FILENAME);
      res = my_rename(file->transactionDeleteFile.c_str(), renameFile.c_str(), 0);
      // If rename was not successful return and rollback
      if (res)
        return res;
      // Set new filename in case another table rollback fails and we need to roll this back
      file->transactionDeleteFile = renameFile;
    } else {
      if (checkFileExists(file->transactionDeleteFile.c_str()))
        my_delete(file->transactionDeleteFile.c_str(), 0);
    }
  }

  this->inProgress = false;
  return res;
}

/**
 * Rolls back a transaction
 * @return
 */
int crunchTxn::rollback() {
  int res = 0;
  for (auto table : this->tables) {
    filesForTransaction *file = table.second;
    if (isFdValid(file->transactionDataFileDescriptor)) {
      res = my_close(file->transactionDataFileDescriptor, 0);
      if (!res)
        file->transactionDataFileDescriptor = 0;
    }
    if (res)
      return res;
    if (checkFileExists(file->transactionDataFile.c_str()))
      res = my_delete(file->transactionDataFile.c_str(), 0);
    if (res)
      return res;
    if (isFdValid(file->transactionDeleteFileDescriptor)) {
      res = my_close(file->transactionDeleteFileDescriptor, 0);
      if (!res)
        file->transactionDeleteFileDescriptor = 0;
    }
    if (res)
      return res;
    if (checkFileExists(file->transactionDeleteFile.c_str()))
      res = my_delete(file->transactionDeleteFile.c_str(), 0);
    if (res)
      return res;
  }
  this->inProgress = false;
  this->tablesInUse = 0;
  return res;
}

int crunchTxn::getTransactionDataFileDescriptor(std::string name) {
  if (this->tables.find(name) != this->tables.end()) {
    return this->tables[name]->transactionDataFileDescriptor;
  }
  return -100;
}

int crunchTxn::getTransactionDeleteFileDescriptor(std::string name) {
  if (this->tables.find(name) != this->tables.end()) {
    return this->tables[name]->transactionDeleteFileDescriptor;
  }
  return -101;
}

std::string crunchTxn::getTransactionDataFile(std::string name) {
  if (this->tables.find(name) != this->tables.end()) {
    return this->tables[name]->transactionDataFile;
  }
  return "";
}

std::string crunchTxn::getTransactionDeleteFile(std::string name) {
  if (this->tables.find(name) != this->tables.end()) {
    return this->tables[name]->transactionDeleteFile;
  }
  return "";
}

std::string crunchTxn::getTransactiondataDirectory(std::string name) {
  if (this->tables.find(name) != this->tables.end()) {
    return this->tables[name]->dataDirectory;
  }
  return "";
}
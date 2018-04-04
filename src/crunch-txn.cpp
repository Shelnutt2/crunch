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
                     uint64_t schemaVersion, std::map<uint8_t, ::crunchy::index> indexes) {
  this->isTxFailed = false;
  this->tablesInUse = 0;

  this->startTimeNanoSeconds = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::high_resolution_clock::now().time_since_epoch()).count();

  uuid = sole::uuid4();
  registerNewTable(name, dataDirectory, transactionDirectory, schemaVersion, indexes);
}

/**
 * Destructor
 */
crunchTxn::~crunchTxn() {
  for (auto &table : this->tables) {
    if (isFdValid(table.second->transactionDataFileDescriptor))
      my_close(table.second->transactionDataFileDescriptor, 0);
    if (isFdValid(table.second->transactionDeleteFileDescriptor))
      my_close(table.second->transactionDeleteFileDescriptor, 0);
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
                                uint64_t schemaVersion, std::map<uint8_t, ::crunchy::index> indexes) {
  // If table already exists don't re-register it
  if (this->tables.find(name) != this->tables.end()) {
    return 0;
  }
  char name_buff[FN_REFLEN];

  // Create new files struct for new table
  std::unique_ptr<filesForTransaction> file = std::make_unique<filesForTransaction>();
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

  for(auto &index : indexes) {
     std::unique_ptr<indexDetailsForTransaction> indexDetails = std::make_unique<indexDetailsForTransaction>();

    indexDetails->indexID = index.first;
    indexDetails->transactionIndexFile = fn_format(name_buff, file->baseFileName.c_str(), file->transactionDirectory.c_str(),
                                                    ("." + std::to_string(index.first) + TABLE_INDEX_EXTENSION).c_str(),
                                                    MY_REPLACE_EXT | MY_UNPACK_FILENAME);
    indexDetails->transactionIndexFileDescriptor = my_create(indexDetails->transactionIndexFile.c_str(), 0,
                                                      O_RDWR | O_TRUNC, MYF(MY_WME));
    file->transactionIndexFiles[index.first] = std::move(indexDetails);
  }

  this->tables[name] = std::move(file);
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
  //For each table we need to create files in disk to hold transaction data
  for (auto &table : this->tables) {
    table.second->baseFileName = std::to_string(this->startTimeNanoSeconds) + "-" + uuid.str();

    if(table.second->transactionDataFile.empty()) {
      table.second->transactionDataFile = fn_format(name_buff, table.second->baseFileName.c_str(), table.second->transactionDirectory.c_str(),
                                            table.second->dataExtension.c_str(),
                                            MY_REPLACE_EXT | MY_UNPACK_FILENAME);

      if (!isFdValid(table.second->transactionDataFileDescriptor)) {
        table.second->transactionDataFileDescriptor = my_create(table.second->transactionDataFile.c_str(), 0,
                                                        O_RDWR | O_TRUNC, MYF(MY_WME));
      }
    }

    if(table.second->transactionDeleteFile.empty()) {
      table.second->transactionDeleteFile = fn_format(name_buff, table.second->baseFileName.c_str(), table.second->transactionDirectory.c_str(),
                                              TABLE_DELETE_EXTENSION,
                                              MY_REPLACE_EXT | MY_UNPACK_FILENAME);

      if (!isFdValid(table.second->transactionDeleteFileDescriptor)) {
        table.second->transactionDeleteFileDescriptor = my_create(table.second->transactionDeleteFile.c_str(), 0,
                                                          O_RDWR | O_TRUNC, MYF(MY_WME));
      }
    }


    for(auto &index : table.second->transactionIndexFiles) {
      if(index.second == nullptr || index.second->transactionIndexFile.empty()) {
        std::unique_ptr<indexDetailsForTransaction> indexDetails = std::make_unique<indexDetailsForTransaction>();

        indexDetails->indexID = index.first;
        indexDetails->transactionIndexFile = fn_format(name_buff, table.second->baseFileName.c_str(),
                                                       table.second->transactionDirectory.c_str(),
                                                       ("." + std::to_string(index.first) +
                                                        TABLE_INDEX_EXTENSION).c_str(),
                                                       MY_REPLACE_EXT | MY_UNPACK_FILENAME);
        indexDetails->transactionIndexFileDescriptor = my_create(indexDetails->transactionIndexFile.c_str(), 0,
                                                                 O_RDWR | O_TRUNC, MYF(MY_WME));
        table.second->transactionIndexFiles[index.first] = std::move(indexDetails);
      }
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

  for (auto &table : this->tables) {
    if (isFdValid(table.second->transactionDataFileDescriptor)) {
      res = my_close(table.second->transactionDataFileDescriptor, 0);
      if (!res)
        table.second->transactionDataFileDescriptor = 0;
    }
    if (getFilesize(table.second->transactionDataFile.c_str()) > 0) {
      std::string renameFile = fn_format(name_buff, table.second->baseFileName.c_str(), table.second->dataDirectory.c_str(),
                                         table.second->dataExtension.c_str(),
                                         MY_REPLACE_EXT | MY_UNPACK_FILENAME);
      res = my_rename(table.second->transactionDataFile.c_str(), renameFile.c_str(), 0);
      // If rename was not successful return and rollback
      if (res)
        return res;
      // Set new filename in case another table rollback fails and we need to roll this back
      table.second->transactionDataFile = renameFile;
    } else {
      if (checkFileExists(table.second->transactionDataFile.c_str()))
        my_delete(table.second->transactionDataFile.c_str(), 0);
    }


    if (isFdValid(table.second->transactionDeleteFileDescriptor)) {
      res = my_close(table.second->transactionDeleteFileDescriptor, 0);
      if (!res)
        table.second->transactionDeleteFileDescriptor = 0;
    }
    if (getFilesize(table.second->transactionDeleteFile.c_str()) > 0) {
      std::string renameFile = fn_format(name_buff, table.second->baseFileName.c_str(), table.second->dataDirectory.c_str(),
                                         TABLE_DELETE_EXTENSION, MY_REPLACE_EXT | MY_UNPACK_FILENAME);
      res = my_rename(table.second->transactionDeleteFile.c_str(), renameFile.c_str(), 0);
      // If rename was not successful return and rollback
      if (res)
        return res;
      // Set new filename in case another table rollback fails and we need to roll this back
      table.second->transactionDeleteFile = renameFile;
    } else {
      if (checkFileExists(table.second->transactionDeleteFile.c_str()))
        my_delete(table.second->transactionDeleteFile.c_str(), 0);
    }

    for(auto &indexFile : table.second->transactionIndexFiles) {
      if (isFdValid(indexFile.second->transactionIndexFileDescriptor)) {
        res = my_close(indexFile.second->transactionIndexFileDescriptor, 0);
        if (!res)
          indexFile.second->transactionIndexFileDescriptor= 0;
      }
      if (res)
        return res;

      if(getFilesize(indexFile.second->transactionIndexFile.c_str()) > 0) {
        std::string renameFile = fn_format(name_buff, table.second->baseFileName.c_str(), table.second->dataDirectory.c_str(),
                                           ("." + std::to_string(indexFile.first) + TABLE_INDEX_EXTENSION).c_str(),
                                           MY_REPLACE_EXT | MY_UNPACK_FILENAME);
        res = my_rename(indexFile.second->transactionIndexFile.c_str(), renameFile.c_str(), 0);
        // If rename was not successful return and rollback
        if (res)
          return res;
        // Set new filename in case another table rollback fails and we need to roll this back
        indexFile.second->transactionIndexFile = renameFile;
      } else {
        if (checkFileExists(indexFile.second->transactionIndexFile.c_str()))
          my_delete(indexFile.second->transactionIndexFile.c_str(), 0);
      }

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
  for (auto &table : this->tables) {
    if (isFdValid(table.second->transactionDataFileDescriptor)) {
      res = my_close(table.second->transactionDataFileDescriptor, 0);
      if (!res)
        table.second->transactionDataFileDescriptor = 0;
    }
    if (res)
      return res;
    if (checkFileExists(table.second->transactionDataFile.c_str()))
      res = my_delete(table.second->transactionDataFile.c_str(), 0);
    if (res)
      return res;
    if (isFdValid(table.second->transactionDeleteFileDescriptor)) {
      res = my_close(table.second->transactionDeleteFileDescriptor, 0);
      if (!res)
        table.second->transactionDeleteFileDescriptor = 0;
    }
    if (res)
      return res;
    if (checkFileExists(table.second->transactionDeleteFile.c_str()))
      res = my_delete(table.second->transactionDeleteFile.c_str(), 0);
    if (res)
      return res;

    for(auto &indexFile : table.second->transactionIndexFiles) {
      if (isFdValid(indexFile.second->transactionIndexFileDescriptor)) {
        res = my_close(indexFile.second->transactionIndexFileDescriptor, 0);
        if (!res)
          indexFile.second->transactionIndexFileDescriptor= 0;
      }
      if (res)
        return res;
      if (checkFileExists(indexFile.second->transactionIndexFile.c_str()))
        res = my_delete(indexFile.second->transactionIndexFile.c_str(), 0);
      if (res)
        return res;

    }
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

int crunchTxn::getTransactionIndexFileDescriptor(std::string name, uint8_t indexID) {
  if (this->tables.find(name) != this->tables.end() &&
      this->tables[name]->transactionIndexFiles.find(indexID) != this->tables[name]->transactionIndexFiles.end()) {
    return this->tables[name]->transactionIndexFiles[indexID]->transactionIndexFileDescriptor;
  }
  return -102;
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

std::string crunchTxn::getTransactionIndexFile(std::string name, uint8_t indexID) {
  if (this->tables.find(name) != this->tables.end() &&
      this->tables[name]->transactionIndexFiles.find(indexID) != this->tables[name]->transactionIndexFiles.end()) {
    return this->tables[name]->transactionIndexFiles[indexID]->transactionIndexFile;
  }
  return "";
}

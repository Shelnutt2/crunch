//
// Created by Seth Shelnutt on 11/10/17.
//

#ifndef MYSQL_CRUNCH_TXN_HPP
#define MYSQL_CRUNCH_TXN_HPP


#include <handler.h>
#include <chrono>
#include <unordered_map>
#include <map>
#include <capnp/schema.h>
#include "sole.hpp"
#include "capnp-mysql.hpp"

typedef struct indexDetailsForTransactionStruct {
    uint8_t indexID;

    std::string transactionIndexFile;

    int transactionIndexFileDescriptor;
} indexDetailsForTransaction;

typedef struct filesForTransaction {
    std::string tableName;

    std::string transactionDirectory;

    std::string transactionDataFile;

    int transactionDataFileDescriptor;

    std::string transactionDeleteFile;

    int transactionDeleteFileDescriptor;

    std::string baseFileName;

    uint64_t schemaVersion;

    std::string dataExtension;

    std::string dataDirectory;

    std::map<uint8_t, std::unique_ptr<indexDetailsForTransaction>> transactionIndexFiles;
} filesForTransaction;

class crunchTxn {


public:
      crunchTxn(std::string name, std::string dataDirectory, std::string transactionDirectory, uint64_t schemaVersion,
                std::map<uint8_t, ::crunchy::index> indexes);

    ~crunchTxn();

    int begin();

    int commitOrRollback();

    int commit();

    int rollback();

    int registerNewTable(std::string name, std::string dataDirectory, std::string transactionDirectory,
                         uint64_t schemaVersion, std::map<uint8_t, ::crunchy::index> indexes);

    int getTransactionDataFileDescriptor(std::string name);

    int getTransactionDeleteFileDescriptor(std::string name);

    int getTransactionIndexFileDescriptor(std::string name, uint8_t indexID);

    std::string getTransactionDataFile(std::string name);

    std::string getTransactionDeleteFile(std::string name);

    std::string getTransactionIndexFile(std::string name, uint8_t indexID);

    std::string getTransactiondataDirectory(std::string name);

    enum_tx_isolation tx_isolation;

    bool isTxFailed;

    bool inProgress;

    sole::uuid uuid;

    int tablesInUse;

    std::unordered_map<std::string, std::unique_ptr<filesForTransaction>> tables;

    std::uint64_t startTimeNanoSeconds;
};


#endif //MYSQL_CRUNCH_TXN_HPP

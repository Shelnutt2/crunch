//
// Created by Seth Shelnutt on 11/10/17.
//

#ifndef MYSQL_CRUNCH_TXN_HPP
#define MYSQL_CRUNCH_TXN_HPP


#include <handler.h>
#include <chrono>
#include <unordered_map>
#include "sole.hpp"

typedef struct filesForTransaction {
    std::string baseDirectory;

    std::string transactionDirectory;

    std::string transactionDataFile;

    int transactionDataFileDescriptor;

    std::string transactionDeleteFile;

    int transactionDeleteFileDescriptor;

    std::string baseFileName;
} filesForTransaction;

class crunchTxn {


public:
    crunchTxn(std::string baseDirectory, std::string transactionDirectory);

    ~crunchTxn();

    int begin();

    int commit_or_rollback();

    int commit();

    int rollback();

    int registerNewTable(std::string baseDirectory, std::string transactionDirectory);

    int getTransactionDataFileDescriptor(std::string name);

    int getTransactionDeleteFileDescriptor(std::string name);

    enum_tx_isolation tx_isolation;

    bool isTxFailed;

    bool inProgress;

    sole::uuid uuid;

    int tablesInUse;

    std::unordered_map<std::string, filesForTransaction*> tables;

    std::chrono::duration<long long int, std::nano> startTimeMilliSeconds;
};


#endif //MYSQL_CRUNCH_TXN_HPP

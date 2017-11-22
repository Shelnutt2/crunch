//
// Created by Seth Shelnutt on 11/10/17.
//

#ifndef MYSQL_CRUNCH_TXN_HPP
#define MYSQL_CRUNCH_TXN_HPP


#include <handler.h>
#include "sole.hpp"

class crunchTxn {

public:
    crunchTxn(std::string baseDirectory, std::string transactionDirectory);

    ~crunchTxn();

    int begin();

    int commit_or_rollback();

    int commit();

    int rollback();

    enum_tx_isolation tx_isolation;

    bool isTxFailed;

    bool inProgress;

    sole::uuid uuid;

    std::string baseDirectory;

    std::string transactionDirectory;

    std::string transactionDataFile;

    int transactionDataFileDescriptor;
    //std::chrono::nanoseconds startTimeMilliSeconds;
    std::string baseFileName;
    std::chrono::duration<long long int, std::nano> startTimeMilliSeconds;
};


#endif //MYSQL_CRUNCH_TXN_HPP

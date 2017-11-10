//
// Created by Seth Shelnutt on 11/10/17.
//

#ifndef MYSQL_CRUNCH_TXN_HPP
#define MYSQL_CRUNCH_TXN_HPP


#include <handler.h>
#include "sole.hpp"

class crunchTxn {

public:
    crunchTxn(){};

    ~crunchTxn() {};

    int begin();

    int commit_or_rollback();

    int commit();

    int rollback();

    enum_tx_isolation tx_isolation;

    bool is_tx_failed;

    sole::uuid uuid;
};


#endif //MYSQL_CRUNCH_TXN_HPP

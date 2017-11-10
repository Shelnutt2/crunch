//
// Created by Seth Shelnutt on 11/10/17.
//

#include "crunch-txn.hpp"

int crunchTxn::begin() {
  uuid = sole::uuid4();
}

int crunchTxn::commit_or_rollback() {
  int res;
  if (this->is_tx_failed) {
    this->rollback();
    res = false;
  } else {
    res = this->commit();
  }
  return res;
}

int crunchTxn::commit() {
  int res;
  return res;
}

int crunchTxn::rollback() {
  int res;
  return res;
}

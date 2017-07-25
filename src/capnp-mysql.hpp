//
// Created by seth on 7/6/17.
//

#ifndef CAPNP_MYSQL_HPP
#define CAPNP_MYSQL_HPP

#include <string>
#include <unordered_map>
#include <map>

#include <capnp/dynamic.h>

#if !_MSC_VER
#include <unistd.h>
#endif
#include <fcntl.h>

#include <field.h>

uint64_t generateRandomId();


std::string buildCapnpSchema(std::map<u_int, std::unordered_map<std::string, std::string>> *columnMap,
                             std::string structName, int *err, uint64_t id = 0);


std::string buildCapnpLimitedSchema(Field **field,
                             std::string structName, int *err, uint64_t id = 0);

std::string getCapnpTypeFromField(Field *field);
#endif //CAPNP_MYSQL_HPP

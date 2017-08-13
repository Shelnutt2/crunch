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

#define NULL_COLUMN_FIELD "nullColumns"

#define ST_MYSQL_TIME_CAPNP "\
struct stMysqlTimeCapnp {\
    year @0 :Uint32;\
    month @1 :Uint32;\
    day @2 :Uint32;\
    hour @3 :Uint32;\
    minute @4 :Uint32;\
    second @5 :Uint32;\
    second_part @6 :UInt64;\
    time_type @7 :enumMysqlTimestampType\
\
    enum enumMysqlTimestampType {\
        MysqlTimestampNone @0;\
        MysqlTimestampError @1;\
        MysqlTimestampDate @2;\
        MysqlTimestampDatetime @3;\
        MysqlTimestampTime @4;\
    };\
}"

typedef struct st_mysql_time
{
    unsigned int  year, month, day, hour, minute, second;
    unsigned long second_part;
    my_bool       neg;
    enum enum_mysql_timestamp_type time_type;
} MYSQL_TIME;

uint64_t generateRandomId();


std::string buildCapnpSchema(std::map<u_int, std::unordered_map<std::string, std::string>> *columnMap,
                             std::string structName, int *err, uint64_t id = 0);


std::string buildCapnpLimitedSchema(Field **field,
                             std::string structName, int *err, uint64_t id = 0);

std::string getCapnpTypeFromField(Field *field);

std::string camelCase(std::string mysqlString);
#endif //CAPNP_MYSQL_HPP

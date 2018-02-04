/*
** Licensed under the GNU Lesser General Public License v3 or later
*/


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
#include <capnp/serialize.h>
#include <memory>

#ifndef NULL_COLUMN_FIELD
#define NULL_COLUMN_FIELD "nullColumns"
#endif

#ifndef CAPNP_SCHEMA_VERSION_COLUMN_FIELD
#define CAPNP_SCHEMA_VERSION_COLUMN_FIELD "capnpSchemaVersion"
#endif

typedef struct schema_struct {
    ::capnp::StructSchema schema;
    uint64_t minimumCompatibleSchemaVersion;
    uint64_t schemaVersion;
} schema;

typedef struct data_struct {
    std::string fileName;
    uint64_t schemaVersion;
} data;

uint64_t generateRandomId();

std::string
buildCapnpLimitedSchema(Field **field, std::string structName, int *err, uint64_t id, uint64_t schemaVersion,
                        uint64_t minimumCompatibleSchemaVersion);

std::string getCapnpTypeFromField(Field *field);

bool checkIfMysqlColumnTypeCapnpCompatible(Field *field1, Field *field2);

std::string camelCase(std::string mysqlString);

std::unique_ptr<capnp::MallocMessageBuilder>
updateMessageToSchema(std::unique_ptr<capnp::FlatArrayMessageReader> message, schema old_schema, schema new_schema);

template <typename T, typename W>
W convert(T, W);


#endif //CAPNP_MYSQL_HPP

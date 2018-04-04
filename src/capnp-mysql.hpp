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
#include <vector>

#ifndef NULL_COLUMN_FIELD
#define NULL_COLUMN_FIELD "nullColumns"
#endif

#ifndef CAPNP_SCHEMA_VERSION_COLUMN_FIELD
#define CAPNP_SCHEMA_VERSION_COLUMN_FIELD "capnpSchemaVersion"
#endif

#ifndef NON_MYSQL_FIELD_COUNT
#define NON_MYSQL_FIELD_COUNT 2
#endif

#ifndef NON_MYSQL_INDEX_FIELD_COUNT
#define NON_MYSQL_INDEX_FIELD_COUNT 2
#endif

#define CRUNCH_ROW_LOCATION_SCHEMA "@0x9dc51f5a1bdb000d;\n\
struct CrunchRowLocation{\n\
    fileName @0 :Text;\n\
    rowStartLocation @1 :UInt64;\n\
    rowEndLocation @2 :UInt64;\n\
}"

#define CRUNCH_ROW_LOCATION_STRUCT_FIELD_NAME "crunchRowLocation"

#define CRUNCH_INDEX_COMBINED_FIELD_NAME "crunchIndexCombined"

typedef struct schema_struct {
    ::capnp::StructSchema schema;
    uint64_t minimumCompatibleSchemaVersion;
    uint64_t schemaVersion;
} schema;

typedef struct data_struct {
    std::string fileName;
    uint64_t schemaVersion;
} data;

typedef struct index_file_struct {
    std::string fileName;
    uint8_t indexID;
    size_t size;
    uint64_t rows;
    ulong flags; /* dupp key and pack flags */
} indexFile;

namespace crunchy {
    typedef struct index_struct {
        ::capnp::StructSchema schema;
        uint64_t indexFlags;
    } index;
}

uint64_t generateRandomId();

std::string
buildCapnpLimitedSchema(std::vector<Field*> fields, std::string structName, int *err, uint64_t id, uint64_t schemaVersion,
                        uint64_t minimumCompatibleSchemaVersion);

std::string buildCapnpIndexSchema(KEY *key_info, std::string structName, int *err, uint64_t id);

std::string getCapnpTypeFromField(Field *field);

bool checkIfMysqlColumnTypeCapnpCompatible(Field *field1, Field *field2);

std::string camelCase(std::string mysqlString);

std::unique_ptr<capnp::MallocMessageBuilder>
updateMessageToSchema(std::unique_ptr<capnp::FlatArrayMessageReader> message, schema old_schema, schema new_schema);

template <typename T, typename W>
W convert(T, W);


#endif //CAPNP_MYSQL_HPP

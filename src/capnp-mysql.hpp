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

#ifndef NULL_COLUMN_FIELD
#define NULL_COLUMN_FIELD "nullColumns"
#endif

#ifndef CAPNP_SCHEMA_VERSION_COLUMN_FIELD
#define CAPNP_SCHEMA_VERSION_COLUMN_FIELD "capnpSchemaVersion"
#endif

uint64_t generateRandomId();

std::string
buildCapnpLimitedSchema(Field **field, std::string structName, int *err, uint64_t id, uint64_t schemaVersion,
                        uint64_t minimumCompatibleSchemaVersion);

std::string getCapnpTypeFromField(Field *field);

std::string camelCase(std::string mysqlString);

#endif //CAPNP_MYSQL_HPP

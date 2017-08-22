/*
** Licensed under the GNU Lesser General Public License v3 or later
*/


#include <string>
#include <unordered_map>
#include <map>

#if !_MSC_VER
#include <unistd.h>
#endif
#include <fcntl.h>
#include <field.h>

#include <kj/debug.h>

#include "capnp-mysql.hpp"
#include <capnp/dynamic.h>


uint64_t generateRandomId() {
  uint64_t result;

#if _WIN32
  HCRYPTPROV handle;
  KJ_ASSERT(CryptAcquireContextW(&handle, nullptr, nullptr,
                                 PROV_RSA_FULL, CRYPT_VERIFYCONTEXT | CRYPT_SILENT));
  KJ_DEFER(KJ_ASSERT(CryptReleaseContext(handle, 0)) {break;});

  KJ_ASSERT(CryptGenRandom(handle, sizeof(result), reinterpret_cast<BYTE*>(&result)));

#else
  int fd;
  KJ_SYSCALL(fd = open("/dev/urandom", O_RDONLY));

  ssize_t n;
  KJ_SYSCALL(n = read(fd, &result, sizeof(result)), "/dev/urandom");
      KJ_ASSERT(n == sizeof(result), "Incomplete read from /dev/urandom.", n);
#endif

  return result | (1ull << 63);
}

std::string getCapnpTypeFromField(Field *field) {
  switch (field->type()) {

    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      return "Float64";

    case MYSQL_TYPE_FLOAT:
      return "Float32";

    case MYSQL_TYPE_TINY:
      return "Int8";
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_YEAR:
      return "Int16";

    case MYSQL_TYPE_INT24:
      return "Int32";

    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
      return "Int64";

    case MYSQL_TYPE_NULL:
      return "Void";

    case MYSQL_TYPE_BIT:
      return "UInt16";


    case MYSQL_TYPE_VARCHAR :
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_SET:
      return "Text";

    case MYSQL_TYPE_GEOMETRY:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_ENUM:
      return "Data";
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
    case MYSQL_TYPE_NEWDATE:
      return "Int64";
  }

  return "Unknown";
}

/** Built a string of a cap'n proto structure from mysql fields list
 *
 * @param fields
 * @param structName
 * @param err
 * @param id
 * @return
 */
std::string buildCapnpLimitedSchema(Field **fields, std::string structName, int *err, uint64_t id) {

  if(id == 0) {
    id = generateRandomId();
  }
  std::string output = kj::str("@0x", kj::hex(id), ";\n").cStr();
  output += "struct " + structName + " {\n";

  output += "  " + std::string(NULL_COLUMN_FIELD) + " @0 :List(Bool);\n";
  for (Field **field = fields; *field; field++)
  {
    output += "  " + camelCase((*field)->field_name) + " @" + std::to_string((*field)->field_index+1)  + " :" + getCapnpTypeFromField(*field) + ";\n";

  }

  output += "}";

  return output;
}

std::string camelCase(std::string mysqlString)  {
  std::string camelString = mysqlString;
  for (uint i = 0; i < camelString.length(); i++){
    if (camelString[i] == '_'){
      std::string tmpString = camelString.substr(i + 1, 1);
      transform(tmpString.begin(), tmpString.end(), tmpString.begin(), toupper);
      camelString.erase(i, 1);
      //camelString.insert(i, tempString);
      for(uint j = 0; j < tmpString.length(); j++) {
        camelString[i+j] = tmpString[j];
      }
    }
  }
  return camelString;
}

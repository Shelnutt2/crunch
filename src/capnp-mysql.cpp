//
// Created by seth on 7/6/17.
//

#ifndef CAPNP_MYSQL_HPP
#define CAPNP_MYSQL_HPP

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
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_YEAR:
      return "Int8";

    case MYSQL_TYPE_INT24:
      return "Int32";

    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
      return "Int64";

    case MYSQL_TYPE_NULL:
      return "Void";

    case MYSQL_TYPE_BIT:
      return "Bool";


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
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
    case MYSQL_TYPE_NEWDATE:
      return "Data";
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
std::string buildCapnpLimitedSchema(Field **fields, std::string structName, int *err, uint64_t id = 0) {

  if(id == 0) {
    id = generateRandomId();
  }
  std::string output = kj::str("@0x", kj::hex(id), ";\n").cStr();
  output += "struct " + structName + " {\n";

  for (Field **field = fields; *field; field++)
  {
    output += "  " + std::string((*field)->field_name) + " @" + std::to_string((*field)->field_index) + " :" + getCapnpTypeFromField(*field) + ";\n";

  }

  output += "}";

  return output;
}

#endif //CAPNP_MYSQL_HPP

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
#include <iostream>
#include <capnp/serialize.h>


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

/**
 * @brief Check if two fields have a compatible capnp data type in the sense that capnp can read
 * either fields data with the other type. Does not account for loss of persision or overflow
 *
 * @param field1
 * @param field2
 * @return
 */
bool checkIfMysqlColumnTypeCapnpCompatible(Field *field1, Field *field2) {
  std::string field1CapnpType = getCapnpTypeFromField(field1);
  std::string field2CapnpType = getCapnpTypeFromField(field2);
  // If they are the same (int16 and int16, or Data/Text/Void return true
  if (field1CapnpType == field2CapnpType)
    return true;

  // Check for float types
  if (field1CapnpType == "Float64" || field1CapnpType == "Float32") {
    if (field2CapnpType == "Float64" || field2CapnpType == "Float32") {
      return true;
    }
    return false;
  }

  // Check for integer types, does not check unsigned ints yet.
  if (field1CapnpType == "Int8" || field1CapnpType == "Int16"
      || field1CapnpType == "Int24" || field1CapnpType == "Int32"
      || field1CapnpType == "Int64") {
    if (field2CapnpType == "Int8" || field2CapnpType == "Int16"
        || field2CapnpType == "Int24" || field2CapnpType == "Int32"
        || field2CapnpType == "Int64") {
      return true;
    }
    return false;
  }

  return false;
}

/** Built a string of a cap'n proto structure from mysql fields list
 *
 * @param fields
 * @param structName
 * @param err
 * @param id
 * @return
 */
std::string
buildCapnpLimitedSchema(Field **fields, std::string structName, int *err, uint64_t id, uint64_t schemaVersion,
                        uint64_t minimumCompatibleSchemaVersion) {

  if (id == 0) {
    id = generateRandomId();
  }
  std::string output = kj::str("@0x", kj::hex(id), ";\n").cStr();
  output += "struct " + structName + " {\n";

  output += "  " + std::string(NULL_COLUMN_FIELD) + " @0 :List(Bool);\n";
  output +=
    "  " + std::string(CAPNP_SCHEMA_VERSION_COLUMN_FIELD) + " @1 :UInt64 = " + std::to_string(schemaVersion) + ";\n";
  for (Field **field = fields; *field; field++) {
    output += "  " + camelCase((*field)->field_name) + " @" + std::to_string((*field)->field_index + NON_MYSQL_FIELD_COUNT) + " :" +
              getCapnpTypeFromField(*field) + ";\n";

  }

  output += "}";

  output +=
    "\n\nconst minimumCompatibleSchemaVersion :UInt64 = " + std::to_string(minimumCompatibleSchemaVersion) + ";";

  return output;
}

std::string camelCase(std::string mysqlString) {
  std::string camelString = mysqlString;
  for (uint i = 0; i < camelString.length(); i++) {
    if (camelString[i] == '_') {
      std::string tmpString = camelString.substr(i + 1, 1);
      transform(tmpString.begin(), tmpString.end(), tmpString.begin(), toupper);
      camelString.erase(i, 1);
      //camelString.insert(i, tempString);
      for (uint j = 0; j < tmpString.length(); j++) {
        camelString[i + j] = tmpString[j];
      }
    }
  }
  return camelString;
}

std::unique_ptr<capnp::MallocMessageBuilder>
updateMessageToSchema(std::unique_ptr<capnp::FlatArrayMessageReader> message, schema oldSchema, schema newSchema) {
  std::unique_ptr<capnp::MallocMessageBuilder> tableRow = std::make_unique<capnp::MallocMessageBuilder>();
  if (message == nullptr) {
    std::cerr << "Warning message passed to updateMessageToSchema was nullptr" << std::endl;
    return nullptr;
  }
  try {
    const capnp::StructSchema::FieldList &oldSchemaFields = oldSchema.schema.getFields();
    const capnp::StructSchema::FieldList &newSchemaFields = newSchema.schema.getFields();

    // Use stored structure
    capnp::DynamicStruct::Builder rowBuilder = tableRow->initRoot<capnp::DynamicStruct>(newSchema.schema);

    capnp::DynamicList::Builder nulls = rowBuilder.init(NULL_COLUMN_FIELD,
                                                        oldSchemaFields.size() - NON_MYSQL_FIELD_COUNT).as<capnp::DynamicList>();

    capnp::DynamicStruct::Reader rowReader = message->getRoot<capnp::DynamicStruct>(oldSchema.schema);
    //Get nulls
    auto nullsReader = rowReader.get(NULL_COLUMN_FIELD).as<capnp::DynamicList>();
    capnp::StructSchema::Field oldSchemaField, newSchemaField;
    //Start at 2 because first two variables are null list and schema version
    for (uint i = NON_MYSQL_FIELD_COUNT; i < oldSchemaFields.size(); i++) {
      if (i >= newSchemaFields.size())
        break;
      oldSchemaField = oldSchemaFields[i];
      newSchemaField = newSchemaFields[i];
      //Check if the field is null, if it is we don't need to set anything
      if (nullsReader[i - NON_MYSQL_FIELD_COUNT].as<bool>()) {
        nulls.set(i - NON_MYSQL_FIELD_COUNT, true);
      } else {
        nulls.set(i - NON_MYSQL_FIELD_COUNT, false);
        if (oldSchemaField.getType() == newSchemaField.getType())
          rowBuilder.set(newSchemaField.getProto().getName(), rowReader.get(oldSchemaField.getProto().getName()));
        else {
          if (oldSchemaField.getType().isBool()) {
            if (newSchemaField.getType().isInt8()
                || newSchemaField.getType().isInt16()
                || newSchemaField.getType().isInt32()
                || newSchemaField.getType().isInt64()
                || newSchemaField.getType().isUInt8()
                || newSchemaField.getType().isUInt16()
                || newSchemaField.getType().isUInt32()
                || newSchemaField.getType().isUInt64()
                || newSchemaField.getType().isFloat32()
                || newSchemaField.getType().isFloat64()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             rowReader.get(oldSchemaField.getProto().getName()).as<bool>());
            } else if (newSchemaField.getType().isText()
                       || newSchemaField.getType().isData()) {
              if (rowReader.get(oldSchemaField.getProto().getName()).as<bool>()) {
                rowBuilder.set(newSchemaField.getProto().getName(), "True");
              } else {
                rowBuilder.set(newSchemaField.getProto().getName(), "False");
              }
            }
          } else if (oldSchemaField.getType().isText()) {
            try {
              if (newSchemaField.getType().isInt8()
                  || newSchemaField.getType().isInt16()) {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::stoi(rowReader.get(oldSchemaField.getProto().getName()).as<capnp::Text>().cStr()));
              } else if (newSchemaField.getType().isInt32()) {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::stoll(rowReader.get(oldSchemaField.getProto().getName()).as<capnp::Text>().cStr()));

              } else if (newSchemaField.getType().isInt64()) {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::stoll(rowReader.get(oldSchemaField.getProto().getName()).as<capnp::Text>().cStr()));
              } else if (newSchemaField.getType().isUInt8()
                         || newSchemaField.getType().isUInt16()
                         || newSchemaField.getType().isUInt32()) {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::stoul(rowReader.get(oldSchemaField.getProto().getName()).as<capnp::Text>().cStr()));
              } else if (newSchemaField.getType().isUInt64()) {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::stoull(
                                 rowReader.get(oldSchemaField.getProto().getName()).as<capnp::Text>().cStr()));

              } else if (newSchemaField.getType().isFloat32()) {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::stof(rowReader.get(oldSchemaField.getProto().getName()).as<capnp::Text>().cStr()));
              } else if (newSchemaField.getType().isFloat64()) {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::stod(rowReader.get(oldSchemaField.getProto().getName()).as<capnp::Text>().cStr()));
              } else if (newSchemaField.getType().isData()) {
                rowBuilder.set(newSchemaField.getProto().getName(), rowReader.get(oldSchemaField.getProto().getName()));
              }
              // If there is an error converting from string to data type, just don't set the value
              // its default mysql behavior
            } catch (const std::exception &e) {
              continue;
            }
            // We can treat int vs uint the same, as we are not changing bits, so the conversion is consistent
          } else if (oldSchemaField.getType().isInt8() || oldSchemaField.getType().isUInt8()) {
            if (newSchemaField.getType().isInt16()
                || newSchemaField.getType().isInt32()
                || newSchemaField.getType().isInt64()
                || newSchemaField.getType().isFloat32()
                || newSchemaField.getType().isFloat64()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             rowReader.get(oldSchemaField.getProto().getName()).as<int8_t>());
            } else if (newSchemaField.getType().isUInt16()
                       || newSchemaField.getType().isUInt32()
                       || newSchemaField.getType().isUInt64()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             rowReader.get(oldSchemaField.getProto().getName()).as<uint8_t>());
            } else if (newSchemaField.getType().isText()
                       || newSchemaField.getType().isData()) {
              // Only string values need to be treated differently
              if (oldSchemaField.getType().isUInt8()) {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::to_string(
                                 rowReader.get(oldSchemaField.getProto().getName()).as<uint8_t>()).c_str());
              } else {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::to_string(rowReader.get(oldSchemaField.getProto().getName()).as<int8_t>()).c_str());
              }
            }
          } else if (oldSchemaField.getType().isInt16() || oldSchemaField.getType().isUInt16()) {
            if (newSchemaField.getType().isInt8()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<int8_t>(rowReader.get(oldSchemaField.getProto().getName()).as<int16_t>()));
            } else if (newSchemaField.getType().isInt32()
                       || newSchemaField.getType().isInt64()
                       || newSchemaField.getType().isFloat32()
                       || newSchemaField.getType().isFloat64()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             rowReader.get(oldSchemaField.getProto().getName()).as<int16_t>());
            } else if (newSchemaField.getType().isUInt8()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<uint8_t>(rowReader.get(oldSchemaField.getProto().getName()).as<int16_t>()));
            } else if (newSchemaField.getType().isUInt32()
                       || newSchemaField.getType().isUInt64()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             rowReader.get(oldSchemaField.getProto().getName()).as<uint16_t>());
            } else if (newSchemaField.getType().isText()
                       || newSchemaField.getType().isData()) {
              // Only string values need to be treated differently
              if (oldSchemaField.getType().isUInt16()) {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::to_string(
                                 rowReader.get(oldSchemaField.getProto().getName()).as<uint16_t>()).c_str());
              } else {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::to_string(
                                 rowReader.get(oldSchemaField.getProto().getName()).as<int16_t>()).c_str());
              }
            }
          } else if (oldSchemaField.getType().isInt32() || oldSchemaField.getType().isUInt32()) {
            if (newSchemaField.getType().isInt8()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<int8_t>(rowReader.get(oldSchemaField.getProto().getName()).as<int32_t>()));
            } else if (newSchemaField.getType().isInt16()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<int16_t>(rowReader.get(oldSchemaField.getProto().getName()).as<int32_t>()));
            } else if (newSchemaField.getType().isInt64()
                       || newSchemaField.getType().isFloat32()
                       || newSchemaField.getType().isFloat64()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             rowReader.get(oldSchemaField.getProto().getName()).as<int32_t>());
            } else if (newSchemaField.getType().isUInt8()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<uint8_t>(rowReader.get(oldSchemaField.getProto().getName()).as<int32_t>()));
            } else if (newSchemaField.getType().isUInt16()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<uint16_t>(rowReader.get(oldSchemaField.getProto().getName()).as<int32_t>()));
            } else if (newSchemaField.getType().isUInt64()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             rowReader.get(oldSchemaField.getProto().getName()).as<uint32_t>());
            } else if (newSchemaField.getType().isText()
                       || newSchemaField.getType().isData()) {
              // Only string values need to be treated differently
              if (oldSchemaField.getType().isUInt32()) {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::to_string(
                                 rowReader.get(oldSchemaField.getProto().getName()).as<uint32_t>()).c_str());
              } else {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::to_string(
                                 rowReader.get(oldSchemaField.getProto().getName()).as<int32_t>()).c_str());
              }
            }
          } else if (oldSchemaField.getType().isInt64() || oldSchemaField.getType().isUInt64()) {
            if (newSchemaField.getType().isInt8()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<int8_t>(rowReader.get(oldSchemaField.getProto().getName()).as<int64_t>()));
            } else if (newSchemaField.getType().isInt16()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<int16_t>(rowReader.get(oldSchemaField.getProto().getName()).as<int64_t>()));
            } else if (newSchemaField.getType().isInt32()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<int32_t>(rowReader.get(oldSchemaField.getProto().getName()).as<int64_t>()));
            } else if (newSchemaField.getType().isFloat32()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<float>(rowReader.get(oldSchemaField.getProto().getName()).as<int64_t>()));
            } else if (newSchemaField.getType().isFloat64()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             rowReader.get(oldSchemaField.getProto().getName()).as<int64_t>());
            } else if (newSchemaField.getType().isUInt8()) {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               static_cast<uint8_t>(rowReader.get(oldSchemaField.getProto().getName()).as<int64_t>()));
              } else if (newSchemaField.getType().isUInt16()) {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               static_cast<uint16_t>(rowReader.get(oldSchemaField.getProto().getName()).as<int64_t>()));
              } else if (newSchemaField.getType().isUInt32()) {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               static_cast<uint32_t>(rowReader.get(oldSchemaField.getProto().getName()).as<int64_t>()));
            } else if (newSchemaField.getType().isText()
                       || newSchemaField.getType().isData()) {
              // Only string values need to be treated differently
              if (oldSchemaField.getType().isUInt64()) {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::to_string(
                                 rowReader.get(oldSchemaField.getProto().getName()).as<uint64_t>()).c_str());
              } else {
                rowBuilder.set(newSchemaField.getProto().getName(),
                               std::to_string(
                                 rowReader.get(oldSchemaField.getProto().getName()).as<int64_t>()).c_str());
              }
            }
          } else if (oldSchemaField.getType().isFloat32()) {
            if (newSchemaField.getType().isInt8()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<int8_t>(rowReader.get(oldSchemaField.getProto().getName()).as<float>()));
            } else if (newSchemaField.getType().isInt16()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<int16_t>(rowReader.get(oldSchemaField.getProto().getName()).as<float>()));
            } else if (newSchemaField.getType().isInt32()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<int32_t>(rowReader.get(oldSchemaField.getProto().getName()).as<float>()));

            } else if (newSchemaField.getType().isInt64()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<int64_t>(rowReader.get(oldSchemaField.getProto().getName()).as<float>()));
            } else if (newSchemaField.getType().isUInt8()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<uint8_t>(rowReader.get(oldSchemaField.getProto().getName()).as<float>()));
            } else if (newSchemaField.getType().isUInt16()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<uint16_t>(rowReader.get(oldSchemaField.getProto().getName()).as<float>()));
            } else if (newSchemaField.getType().isUInt32()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<uint32_t>(rowReader.get(oldSchemaField.getProto().getName()).as<float>()));
            } else if (newSchemaField.getType().isUInt64()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<uint64_t>(rowReader.get(oldSchemaField.getProto().getName()).as<float>()));

            } else if (newSchemaField.getType().isFloat64()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<double>(rowReader.get(oldSchemaField.getProto().getName()).as<float>()));
            } else if (newSchemaField.getType().isData()
                       || newSchemaField.getType().isText()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             std::to_string(rowReader.get(oldSchemaField.getProto().getName()).as<float>()).c_str());
            }
          } else if (oldSchemaField.getType().isFloat64()) {
            if (newSchemaField.getType().isInt8()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<int8_t>(rowReader.get(oldSchemaField.getProto().getName()).as<double>()));
            } else if (newSchemaField.getType().isInt16()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<int16_t>(rowReader.get(oldSchemaField.getProto().getName()).as<double>()));
            } else if (newSchemaField.getType().isInt32()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<int32_t>(rowReader.get(oldSchemaField.getProto().getName()).as<double>()));

            } else if (newSchemaField.getType().isInt64()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<int64_t>(rowReader.get(oldSchemaField.getProto().getName()).as<double>()));
            } else if (newSchemaField.getType().isUInt8()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<uint8_t>(rowReader.get(oldSchemaField.getProto().getName()).as<double>()));
            } else if (newSchemaField.getType().isUInt16()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<uint16_t>(rowReader.get(oldSchemaField.getProto().getName()).as<double>()));
            } else if (newSchemaField.getType().isUInt32()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<uint32_t>(rowReader.get(oldSchemaField.getProto().getName()).as<double>()));
            } else if (newSchemaField.getType().isUInt64()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<uint64_t>(rowReader.get(oldSchemaField.getProto().getName()).as<double>()));

            } else if (newSchemaField.getType().isFloat32()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             static_cast<float>(rowReader.get(oldSchemaField.getProto().getName()).as<double>()));
            } else if (newSchemaField.getType().isData()
                       || newSchemaField.getType().isText()) {
              rowBuilder.set(newSchemaField.getProto().getName(),
                             std::to_string(rowReader.get(oldSchemaField.getProto().getName()).as<double>()).c_str());
            }
          }
        }
      }
    }
  } catch (kj::Exception &e) {
    std::cerr << "exception on updateMessageToSchema: " << e.getFile() << ":" << e.getLine()
              << ", line: " << __FILE__ << ":" << __LINE__
              << ", type: " << (int) e.getType()
              << ", e.what(): " << e.getDescription().cStr() << std::endl;
    return nullptr;
  } catch (std::exception &e) {
    std::cerr << "exception on updateMessageToSchema: " << __FILE__ << ":" << __LINE__ << ", e.what(): "
              << e.what() << std::endl;
    return nullptr;
  }

  return
    std::move(tableRow);
}

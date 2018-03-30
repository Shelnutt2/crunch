/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include "crunch.hpp"

#include <iostream>
#include <string>

#include "capnp-mysql.hpp"
#include "crunch-sysvars.hpp"
#include "utils.hpp"
#include "crunch-alter-ctx.hpp"
#include <sys/mman.h>
#include <sql_priv.h>
#include <sql_class.h>
#include <regex>
#include <fstream>
#include <cstdint>

#ifdef UNKNOWN
#undef UNKNOWN
#endif

static int crunch_commit(handlerton *hton, THD *thd, bool all);

static int crunch_rollback(handlerton *hton, THD *thd, bool all);

// Handler for crunch engine
handlerton *crunch_hton;

// crunch file extensions
static const char *crunch_exts[] = {
    TABLE_SCHEMA_EXTENSION,
    TABLE_DATA_EXTENSION,
    TABLE_DELETE_EXTENSION,
    TABLE_INDEX_SCHEMA_EXTENSION,
    TABLE_INDEX_EXTENSION,
    NullS
};


// Create crunch object
static handler *crunch_create_handler(handlerton *hton,
                                      TABLE_SHARE *table,
                                      MEM_ROOT *mem_root) {
  return new(mem_root) crunch(hton, table);
}

// Initialization function
static int crunch_init_func(void *p) {
  DBUG_ENTER("crunch_init_func");

#ifdef HAVE_PSI_INTERFACE
  //init_example_psi_keys();
#endif

  crunch_hton = (handlerton *) p;
  crunch_hton->state = SHOW_OPTION_YES;
  crunch_hton->create = crunch_create_handler;
  crunch_hton->flags = HTON_CAN_RECREATE;
  crunch_hton->tablefile_extensions = crunch_exts;
  crunch_hton->commit = crunch_commit;
  crunch_hton->rollback = crunch_rollback;
  crunch_hton->table_options = crunch_table_options;

  DBUG_RETURN(0);
}

// Storage engine interface
struct st_mysql_storage_engine crunch_storage_engine =
    {MYSQL_HANDLERTON_INTERFACE_VERSION};


bool crunch::mmapData(std::string fileName) {
  DBUG_ENTER("crunch::mmapData");
  currentDataFile = fileName;
  // Get size of data file needed for mmaping
  dataFileSize = getFilesize(fileName.c_str());
  // Only mmap if we have data
  if (isFdValid(dataFileDescriptor)) {
    my_close(dataFileDescriptor, 0);
    dataFileDescriptor = 0;
  }
  dataFileDescriptor = my_open(fileName.c_str(), mode, 0);
  if (dataFileSize > 0) {
    DBUG_PRINT("crunch::mmap", ("Entering"));
    dataPointer = (capnp::word *) mmap(NULL, dataFileSize, PROT_READ, MAP_SHARED, dataFileDescriptor, 0);

    if ((void *) dataPointer == MAP_FAILED) {
      perror("Error ");
      DBUG_PRINT("crunch::mmap", ("Error: %s", strerror(errno)));
      std::cerr << "mmaped failed for table " << name << ", file: " << fileName << " , error: " << strerror(errno)
                << std::endl;
      my_close(dataFileDescriptor, 0);
      dataFileDescriptor = 0;
      DBUG_RETURN(false);
    }

    // Set the start pointer to the current dataPointer
    dataFileStart = dataPointer;
  } else {
    dataPointer = dataFileStart = NULL;
  }
  DBUG_RETURN(true);
}

bool crunch::unmmapData() {
  DBUG_ENTER("crunch::unmmapData");
  if (dataFileSize > 0 && dataFileStart != NULL && munmap((void *) dataFileStart, dataFileSize) == -1) {
    perror("Error un-mmapping the file");
    DBUG_PRINT("crunch::mremapData", ("Error: %s", strerror(errno)));
    return false;
  }
  int res = my_close(dataFileDescriptor, 0);
  if (!res)
    dataFileDescriptor = 0;
  DBUG_RETURN(res);
}

bool crunch::mremapData(std::string fileName) {
  DBUG_ENTER("crunch::mremapData");
#ifdef __linux // Only linux support mremap call
  int oldDataFileSize = dataFileSize;
  // Get size of data file needed for mremaping
  dataFileSize = getFilesize(fileName.c_str());
  // Only mmap if we have data
  if (oldDataFileSize > 0 && dataFileStart != NULL) {
    DBUG_PRINT("crunch::mremap", ("Entering"));

    dataPointer = (capnp::word *) mremap((void *) dataFileStart, oldDataFileSize, dataFileSize, MREMAP_MAYMOVE);
    if ((void *) dataPointer == MAP_FAILED) {
      perror("Error ");
      DBUG_PRINT("crunch::mremap", ("Error: %s", strerror(errno)));
      std::cerr << "mmaped failed for table " << name << ", file: " << currentDataFile << " , error: "
                << strerror(errno) << std::endl;
      my_close(dataFileDescriptor, 0);
      dataFileDescriptor = 0;
      DBUG_RETURN(false);
    }

    // Set the start pointer to the current dataPointer
    dataFileStart = dataPointer;
  } else {
    dataPointer = dataFileStart = NULL;
    DBUG_RETURN(mmapData(fileName));
  }
  DBUG_RETURN(true);
#else
  if (dataFileSize > 0 && dataFileStart != NULL && munmap((void *) dataFileStart, dataFileSize) == -1) {
    perror("Error un-mmapping the file");
    DBUG_PRINT("crunch::mremapData", ("Error: %s", strerror(errno)));
    DBUG_RETURN(false);
  }
  DBUG_RETURN(mmapData(fileName));
#endif
}

bool crunch::capnpDataToMysqlBuffer(uchar *buf, capnp::DynamicStruct::Reader dynamicStructReader) {
  bool ret = true;
  try {
    //Get nulls
    auto nulls = dynamicStructReader.get(NULL_COLUMN_FIELD).as<capnp::DynamicList>();

    // Loop through each field to get the data
    unsigned int colNumber = 0;
    std::vector<bool> nullBits;
    for (Field **field = table->field; *field; field++, colNumber++) {
      std::string capnpFieldName = camelCase((*field)->field_name);

      // Handle when a new field was added but it is not in the data set
      // Must return default value
      if (nulls.size() <= colNumber) {
        if ((*field)->maybe_null()) {
          (*field)->set_null();
          continue;
        } else {
          (*field)->set_notnull();
          (*field)->set_default();
          continue;
        }
      } else if (nulls[colNumber].as<bool>()) {
        (*field)->set_null();
      } else if (dynamicStructReader.has(capnpFieldName)) {
        (*field)->set_notnull();
        auto capnpField = dynamicStructReader.get(capnpFieldName);

        switch (capnpField.getType()) {
          case capnp::DynamicValue::VOID:
            break;
          case capnp::DynamicValue::BOOL:
            (*field)->store(capnpField.as<bool>());
            break;
          case capnp::DynamicValue::INT:
            (*field)->store(capnpField.as<int64_t>(), false);
            break;
          case capnp::DynamicValue::UINT:
            (*field)->store(capnpField.as<uint64_t>(), true);
            break;
          case capnp::DynamicValue::FLOAT:
            (*field)->store(capnpField.as<double>());
            break;
          case capnp::DynamicValue::DATA: {
            kj::ArrayPtr<const char> chars;
            chars = capnpField.as<capnp::Data>().asChars();
            (*field)->store(chars.begin(), chars.size(), &my_charset_utf8_general_ci);
            break;
          }
          case capnp::DynamicValue::TEXT: {
            const char *row_string = capnpField.as<capnp::Text>().cStr();
            (*field)->store(row_string, strlen(row_string), &my_charset_utf8_general_ci);
            break;
          }
          case capnp::DynamicValue::LIST:
          case capnp::DynamicValue::ENUM:
          case capnp::DynamicValue::STRUCT:
          case capnp::DynamicValue::CAPABILITY:
          case capnp::DynamicValue::UNKNOWN:
          case capnp::DynamicValue::ANY_POINTER:
            break;
        }
      } else {
        (*field)->set_null();
      }
    }
  } catch (kj::Exception &e) {
    std::cerr << "exception on capnpDataToMysqlBuffer " << name << ": datafile: " << currentDataFile
              << ", kj exception file" << e.getFile() << ", line: " << __FILE__ << ":" << __LINE__
              << ", exception_line: " << e.getLine() << ", type: " << (int) e.getType()
              << ", e.what(): " << e.getDescription().cStr() << std::endl;
    return false;
  } catch (std::exception &e) {
    std::cerr << "exception on capnpDataToMysqlBuffer " << name << ": datafile: " << currentDataFile
              << ", line: " << __FILE__ << ":" << __LINE__ << ", e.what(): " << e.what() << std::endl;
    return false;
  }
  return ret;
}

int crunch::rnd_init(bool scan) {
  DBUG_ENTER("crunch::rnd_init");
  // Lock basic mutex
  mysql_mutex_lock(&share->mutex);
  // Reset row number
  // Reset starting mmap position
  int ret = findTableFiles(name);
  if (ret)
    DBUG_RETURN(ret);
  if (dataFiles.size() == 0) {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }
  if (currentDataFile != dataFiles[0].fileName || dataPointer == NULL) {
    unmmapData();
    dataFileIndex = 0;
    data dataStruct = dataFiles[dataFileIndex];
    mmapData(dataStruct.fileName);
  }

  data dataStruct = dataFiles[dataFileIndex];
  if (dataFiles.size() > 0) {
    currentDataFile = dataStruct.fileName;
    capnpRowSchema = capnpRowSchemas.rbegin()->second;
  }

  dataPointer = dataFileStart;
  dataPointerNext = dataFileStart;

  DBUG_RETURN(ret);
}

std::unique_ptr<capnp::FlatArrayMessageReader> crunch::rnd_row(int *err) {
  DBUG_ENTER("crunch::rnd_row");
  //Set datapointer
  dataPointer = dataPointerNext;

  // Before reading we make sure we have not reached the end of the mmap'ed space, which is the end of the file on disk
  if (dataPointer != dataFileStart + (dataFileSize / sizeof(capnp::word))) {
    //Read data
    auto tmpDataMessageReader = std::make_unique<capnp::FlatArrayMessageReader>(
        kj::ArrayPtr<const capnp::word>(dataPointer, dataPointer + (dataFileSize / sizeof(capnp::word))));
    dataPointerNext = tmpDataMessageReader->getEnd();
    uint64_t rowStartLocation = (dataPointer - dataFileStart);
    if (!checkForDeletedRow(currentDataFile, rowStartLocation)) {
      DBUG_RETURN(tmpDataMessageReader);
      //currentRowNumber++;
    } else {
      DBUG_RETURN(rnd_row(err));
    }
  } else { //End of data file
    if (dataFileIndex >= dataFiles.size() - 1) {
      *err = HA_ERR_END_OF_FILE;
      DBUG_RETURN(NULL);
    } else {
      DBUG_PRINT("info", ("End of file, moving to next"));
      unmmapData();
      dataFileIndex++;

      data dataStruct = dataFiles[dataFileIndex];
      DBUG_PRINT("info", ("Next file in rnd_next: %s", dataStruct.fileName.c_str()));
      mmapData(dataStruct.fileName);
      dataPointer = dataFileStart;
      dataPointerNext = dataFileStart;
      currentDataFile = dataStruct.fileName;

      capnpRowSchema = capnpRowSchemas.rbegin()->second;
      if (!*err)
        DBUG_RETURN(rnd_row(err));
    }
  }
  DBUG_RETURN(NULL);
}

int crunch::rnd_next(uchar *buf) {
  int rc = 0;
  DBUG_ENTER("crunch::rnd_next");

  // We must set the bitmap for debug purpose, it is "write_set" because we use Field->store
  my_bitmap_map *orig = dbug_tmp_use_all_columns(table, table->write_set);

  try {
    std::unique_ptr<capnp::FlatArrayMessageReader> rowRead = rnd_row(&rc);
    if (!rc) {
      if (rowRead == nullptr) {
        rc = -41;
      } else {
        uint64_t fileSchemaVersion = dataFiles[dataFileIndex].schemaVersion;
        if (fileSchemaVersion < capnpRowSchema.minimumCompatibleSchemaVersion) {
          schema maxCompatibleSchema = capnpRowSchemas[fileSchemaVersion];
          for (auto schemaIt = capnpRowSchemas.rbegin(); schemaIt != capnpRowSchemas.rend(); ++schemaIt) {
            if (schemaIt->second.minimumCompatibleSchemaVersion <= fileSchemaVersion) {
              maxCompatibleSchema = schemaIt->second;
              break;
            }
          }
          try {
            auto newMessage = updateMessageToSchema(std::move(rowRead), maxCompatibleSchema, capnpRowSchema);
            if (newMessage == nullptr)
              rc = -42;
            else
              rowRead = std::make_unique<capnp::FlatArrayMessageReader>(
                  capnp::messageToFlatArray(newMessage->getSegmentsForOutput()).asPtr());

          } catch (kj::Exception &e) {
            std::cerr << "exception on rnd_next " << name << ": schemaVersion=" << capnpRowSchema.schemaVersion
                      << ", dataFile=" << currentDataFile << ", file version=" << dataFiles[dataFileIndex].schemaVersion
                      << ", row_start_location: " << (dataPointer - dataFileStart)
                      << ", line: " << __FILE__ << ":" << __LINE__
                      << ", error line" << e.getFile() << ":" << e.getLine() << ", type: " << (int) e.getType()
                      << ", e.what(): " << e.getDescription().cStr() << std::endl;
            rc = -43;
          } catch (std::exception &e) {
            std::cerr << "exception on rnd_next " << name << ", line: " << __FILE__ << ":" << __LINE__ << ", e.what(): "
                      << e.what() << std::endl;
            rc = -44;
          }
        }

        if (!rc && !capnpDataToMysqlBuffer(buf, rowRead->getRoot<capnp::DynamicStruct>(capnpRowSchema.schema)))
          rc = -45;
      }
    }
  } catch (kj::Exception &e) {
    std::cerr << "exception on rnd_next " << name << ": schemaVersion=" << capnpRowSchema.schemaVersion
              << ", dataFile=" << currentDataFile << ", file version=" << dataFiles[dataFileIndex].schemaVersion
              << ", row_start_location: " << (dataPointer - dataFileStart)
              << ", line: " << __FILE__ << ":" << __LINE__
              << ", error line" << e.getFile() << ":" << e.getLine() << ", type: " << (int) e.getType()
              << ", e.what(): " << e.getDescription().cStr() << std::endl;
    rc = -46;
  } catch (std::exception &e) {
    std::cerr << "exception on rnd_next " << name << ", line: " << __FILE__ << ":" << __LINE__ << ", e.what(): "
              << e.what() << std::endl;
    rc = -47;
  }

  // Reset bitmap to original
  dbug_tmp_restore_column_map(table->write_set, orig);
  DBUG_RETURN(rc);
}

int crunch::rnd_pos(uchar *buf, uchar *pos) {
  int rc = 0;
  DBUG_ENTER("crunch::rnd_pos");

  // We must set the bitmap for debug purpose, it is "write_set" because we use Field->store
  my_bitmap_map *orig = dbug_tmp_use_all_columns(table, table->write_set);

  try {
    uint64_t len;
    memcpy(&len, pos, sizeof(uint64_t));
    kj::ArrayPtr<const unsigned char> bytes = kj::arrayPtr(pos + sizeof(uint64_t), len);

    const kj::ArrayPtr<const capnp::word> view{
        reinterpret_cast<const capnp::word *>(bytes.begin()),
        reinterpret_cast<const capnp::word *>(bytes.end())};

    capnp::FlatArrayMessageReader message(view);
    CrunchRowLocation::Reader rowLocation = message.getRoot<CrunchRowLocation>();
    if (currentDataFile != std::string(rowLocation.getFileName().cStr())) {
      DBUG_PRINT("info", ("rnd_pos is in different file"));
      unmmapData();
      for (unsigned long i = 0; i < dataFiles.size(); i++) {
        if (dataFiles[i].fileName == std::string(rowLocation.getFileName().cStr())) {
          dataFileIndex = i;
          break;
        }
      }
      data dataStruct = dataFiles[dataFileIndex];
      DBUG_PRINT("info", ("Next file in rnd_next: %s", dataStruct.fileName.c_str()));
      mmapData(dataStruct.fileName);
      capnpRowSchema = capnpRowSchemas.rbegin()->second;
      dataPointer = dataFileStart;
      dataPointerNext = dataFileStart;
    }
    dataPointer = dataFileStart + rowLocation.getRowStartLocation();
    if (!checkForDeletedRow(rowLocation.getFileName().cStr(), rowLocation.getRowStartLocation())) {
      auto tmpDataMessageReader = std::unique_ptr<capnp::FlatArrayMessageReader>(
          new capnp::FlatArrayMessageReader(
              kj::ArrayPtr<const capnp::word>(dataPointer, dataPointer + (dataFileSize / sizeof(capnp::word)))));
      try {
        if (!rc) {
          if (tmpDataMessageReader == nullptr) {
            rc = -51;
          } else {
            uint64_t fileSchemaVersion = dataFiles[dataFileIndex].schemaVersion;
            if (fileSchemaVersion < capnpRowSchema.minimumCompatibleSchemaVersion) {
              schema maxCompatibleSchema = capnpRowSchemas[fileSchemaVersion];
              for (auto schemaIt = capnpRowSchemas.rbegin();
                   schemaIt != capnpRowSchemas.rend(); ++schemaIt) {
                if (schemaIt->second.minimumCompatibleSchemaVersion <= fileSchemaVersion) {
                  maxCompatibleSchema = schemaIt->second;
                  break;
                }
              }
              auto newMessage = updateMessageToSchema(std::move(tmpDataMessageReader),
                                                      maxCompatibleSchema,
                                                      capnpRowSchema);
              if (newMessage == nullptr)
                rc = -52;
              else
                tmpDataMessageReader = std::make_unique<capnp::FlatArrayMessageReader>(
                    capnp::messageToFlatArray(newMessage->getSegmentsForOutput()).asPtr());

            }

            if (!rc && !capnpDataToMysqlBuffer(buf,
                                               tmpDataMessageReader->getRoot<capnp::DynamicStruct>(
                                                   capnpRowSchema.schema)))
              rc = -53;
          }
        }
      } catch (kj::Exception &e) {
        std::cerr << "exception on rnd_next " << name << ": " << e.getFile() << ", line: " << __FILE__ << ":"
                  << __LINE__
                  << ", exception_line: "
                  << e.getLine() << ", type: " << (int) e.getType()
                  << ", e.what(): " << e.getDescription().cStr() << std::endl;
        rc = -54;
      } catch (std::exception &e) {
        std::cerr << "exception on rnd_next " << name << ", line: " << __FILE__ << ":" << __LINE__
                  << ", e.what(): "
                  << e.what() << std::endl;
        rc = -55;
      }
    } else {
      rc = HA_ERR_RECORD_DELETED;
    }
  } catch (kj::Exception e) {
    std::cerr << "exception on table " << name << ": " << e.getFile() << ", line: " << __FILE__ << ":" << __LINE__
              << ", exception_line: "
              << e.getLine() << ", type: " << (int) e.getType()
              << ", e.what(): " << e.getDescription().cStr() << std::endl;
  };
  // Reset bitmap to original
  dbug_tmp_restore_column_map(table->write_set, orig);
  DBUG_RETURN(rc);
}

int crunch::rnd_end() {
  DBUG_ENTER("crunch::rnd_end");
  // Unlock basic mutex
  mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(0);
}

void crunch::build_row(capnp::DynamicStruct::Builder *row, capnp::DynamicList::Builder *nulls) {
  // Loop through each field to write row

  try {
    int index = 0;
    for (Field **field = table->field; *field; field++) {
      std::string capnpFieldName = camelCase((*field)->field_name);
      if ((*field)->is_null()) {
        nulls->set(index++, true);
      } else {
        nulls->set(index++, false);
        switch ((*field)->type()) {

          case MYSQL_TYPE_DOUBLE: {
            row->set(capnpFieldName, (*field)->val_real());
            break;
          }
          case MYSQL_TYPE_DECIMAL:
          case MYSQL_TYPE_NEWDECIMAL: {
            row->set(capnpFieldName, (*field)->val_real());
            break;
          }

          case MYSQL_TYPE_FLOAT: {
            row->set(capnpFieldName, (*field)->val_real());
            break;
          }

          case MYSQL_TYPE_TINY:
          case MYSQL_TYPE_SHORT:
          case MYSQL_TYPE_YEAR:
          case MYSQL_TYPE_INT24:
          case MYSQL_TYPE_LONG:
          case MYSQL_TYPE_LONGLONG: {
            row->set(capnpFieldName, (*field)->val_int());
            break;
          }

          case MYSQL_TYPE_NULL: {
            row->set(capnpFieldName, capnp::DynamicValue::VOID);
            break;
          }

          case MYSQL_TYPE_BIT: {
            row->set(capnpFieldName, (*field)->val_int());
            break;
          }

          case MYSQL_TYPE_VARCHAR:
          case MYSQL_TYPE_STRING:
          case MYSQL_TYPE_VAR_STRING:
          case MYSQL_TYPE_SET: {
            char attribute_buffer[1024];
            String attribute(attribute_buffer, sizeof(attribute_buffer),
                             &my_charset_utf8_general_ci);
            (*field)->val_str(&attribute, &attribute);
            capnp::Text::Reader text = attribute.c_ptr_safe();
            row->set(capnpFieldName, text);
            break;
          }

          case MYSQL_TYPE_GEOMETRY:
          case MYSQL_TYPE_BLOB:
          case MYSQL_TYPE_LONG_BLOB:
          case MYSQL_TYPE_MEDIUM_BLOB:
          case MYSQL_TYPE_TINY_BLOB:
          case MYSQL_TYPE_ENUM: {
            char attribute_buffer[1024];
            String attribute(attribute_buffer, sizeof(attribute_buffer),
                             &my_charset_bin);
            (*field)->val_str(&attribute, &attribute);

            kj::ArrayPtr<kj::byte> bufferPtr = kj::arrayPtr(attribute.c_ptr_safe(),
                                                            attribute.length()).asBytes();
            capnp::Data::Reader data(bufferPtr.begin(), bufferPtr.size());
            row->set(capnpFieldName, data);
            break;
          }
          case MYSQL_TYPE_DATE:
          case MYSQL_TYPE_DATETIME:
          case MYSQL_TYPE_DATETIME2:
          case MYSQL_TYPE_TIME:
          case MYSQL_TYPE_TIME2:
          case MYSQL_TYPE_TIMESTAMP:
          case MYSQL_TYPE_TIMESTAMP2:
          case MYSQL_TYPE_NEWDATE: {
            row->set(capnpFieldName, (*field)->val_int());
            break;
          }
        }
      }
    }
  } catch (kj::Exception e) {
    std::cerr << "exception on table " << name << ", line: " << __FILE__ << ":" << __LINE__
              << ", exception_line: " << e.getFile() << ":" << e.getLine()
              << ", type: " << (int) e.getType()
              << ", e.what(): " << e.getDescription().cStr() << std::endl;
  } catch (const std::exception &e) {
    // Log errors
    std::cerr << "write error for table " << name << ": " << e.what() << std::endl;
  }

}

int crunch::write_buffer(uchar *buf) {
  int ret = 0;
  // We must set the bitmap for debug purpose, it is "write_set" because we use Field->store
  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);

  // Use a message builder for reach row
  try {
    crunchTxn *txn = (crunchTxn *) thd_get_ha_data(ha_thd(), crunch_hton);

    schema schemaForMessage = capnpRowSchemas.rbegin()->second;

    std::shared_ptr<capnp::MallocMessageBuilder> tableRow = std::make_unique<capnp::MallocMessageBuilder>();

    // Use stored structure
    capnp::DynamicStruct::Builder row = tableRow->initRoot<capnp::DynamicStruct>(schemaForMessage.schema);

    capnp::DynamicList::Builder nulls = row.init(NULL_COLUMN_FIELD, numFields).as<capnp::DynamicList>();

    build_row(&row, &nulls);

    ret = write_message(tableRow, txn);

    if (!ret) {
      ret = build_and_write_indexes(tableRow, schemaForMessage, txn);
    }
  } catch (kj::Exception e) {
    std::cerr << "exception on table " << name << ", line: " << __FILE__ << ":" << __LINE__
              << ", exception_line: " << e.getFile() << ":" << e.getLine()
              << ", type: " << (int) e.getType()
              << ", e.what(): " << e.getDescription().cStr() << std::endl;
    ret = -301;
  } catch (const std::exception &e) {
    // Log errors
    std::cerr << "write error for table " << name << ": " << e.what() << std::endl;
    ret = 301;
  }

  // Reset bitmap to original
  dbug_tmp_restore_column_map(table->read_set, old_map);

  return ret;
}

int crunch::write_message(std::shared_ptr<capnp::MallocMessageBuilder> tableRow, crunchTxn *txn) {

  // Use a message builder for reach row
  try {

    int fd = txn->getTransactionDataFileDescriptor(this->name);
    // Set the fileDescriptor to the end of the file
    lseek(fd, 0, SEEK_END);
    //Write message to file
    capnp::writeMessageToFd(fd, *tableRow);

  } catch (kj::Exception e) {
    std::cerr << "exception on table " << name << ", line: " << __FILE__ << ":" << __LINE__
              << ", exception_line: " << e.getFile() << ":" << e.getLine()
              << ", type: " << (int) e.getType()
              << ", e.what(): " << e.getDescription().cStr() << std::endl;
    txn->isTxFailed = true;
    return -322;
  } catch (const std::exception &e) {
    // Log errors
    std::cerr << "write error for table " << name << ": " << e.what() << std::endl;
    txn->isTxFailed = true;
    return 322;
  }

  return 0;
}

int crunch::write_row(uchar *buf) {

  DBUG_ENTER("crunch::write_row");
  // Lock basic mutex
  mysql_mutex_lock(&share->mutex);

  int ret = write_buffer(buf);
  // Unlock basic mutex
  mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(ret);
}

int crunch::delete_row(const uchar *buf) {
  DBUG_ENTER("crunch::delete_row");

  //todo: check for if delete file exists
  //use crunch-delete to handle all logic, just call crunch_delete(current_row_message, offset start, end)
  //crunch-delete will create file if not exists and serialize with capnproto
  uint64_t rowStartLocation = (dataPointer - dataFileStart);
  uint64_t rowEndLocation = (dataPointerNext - dataFileStart);
  markRowAsDeleted(currentDataFile, rowStartLocation, rowEndLocation);

  DBUG_RETURN(0);
}

/**
 * Update row by deleting old row and inserting new data
 * @param old_data
 * @param new_data
 * @return
 */
int crunch::update_row(const uchar *old_data, uchar *new_data) {
  DBUG_ENTER("crunch::update_row");
  // Try to delete row
  int ret = delete_row(old_data);
  if (ret)
    DBUG_RETURN(ret);
  // If delete was successful, write new row
  ret = write_buffer(new_data);
  DBUG_RETURN(ret);
}

/**
 * In the case of an order by rows will need to be sorted.
  ::position() is called after each call to ::rnd_next(),
  the data it stores is to a byte array. You can store this
  data via my_store_ptr(). ref_length is a variable defined to the
  class that is the sizeof() of position being stored.

 * @param record
 */
void crunch::position(const uchar *record) {
  DBUG_ENTER("crunch::position");
  try {

    capnp::MallocMessageBuilder RowLocation;
    CrunchRowLocation::Builder builder = RowLocation.initRoot<CrunchRowLocation>();

    builder.setFileName(currentDataFile);
    uint64_t rowStartLocation = (dataPointer - dataFileStart);
    uint64_t rowEndLocation = (dataPointerNext - dataFileStart);
    builder.setRowEndLocation(rowEndLocation);
    builder.setRowStartLocation(rowStartLocation);

    kj::Array<capnp::word> flatArrayOfLocation = capnp::messageToFlatArray(RowLocation);
    uint64_t len = flatArrayOfLocation.asBytes().size();
    memcpy(ref, &len, sizeof(uint64_t));
    memcpy(ref + sizeof(uint64_t), flatArrayOfLocation.asBytes().begin(), len);
  } catch (kj::Exception e) {
    std::cerr << "exception on table " << name << ", line: " << __FILE__ << ":" << __LINE__
              << ", exception_line: " << e.getFile() << ":" << e.getLine()
              << ", type: " << (int) e.getType()
              << ", e.what(): " << e.getDescription().cStr() << std::endl;
  };
  DBUG_VOID_RETURN;
}

int crunch::start_stmt(THD *thd, thr_lock_type lock_type) {
  DBUG_ENTER("crunch::start_stmt");
  int ret = 0;

  crunchTxn *txn = (crunchTxn *) thd_get_ha_data(thd, crunch_hton);
  if (txn == NULL) {
    txn = new crunchTxn(name, dataFolder, transactionDirectory, schemaVersion, indexSchemas);
    thd_set_ha_data(thd, crunch_hton, txn);
  }

  if (!txn->inProgress) {
    ret = txn->begin();
    //txn->stmt= txn->new_savepoint();
    trans_register_ha(thd, thd->in_multi_stmt_transaction_mode(), crunch_hton);
  } else if (txn->inProgress) {
    txn->registerNewTable(name, dataFolder, transactionDirectory, schemaVersion, indexSchemas);
  }
  DBUG_RETURN(ret);
}

int crunch::info(uint) {
  DBUG_ENTER("crunch::info");
  /* This is a lie, but you don't want the optimizer to see zero or 1 */
  if (stats.records < 2)
    stats.records = 2;
  DBUG_RETURN(0);
}

/** @brief
  This is a bitmap of flags that indicates how the storage engine
  implements indexes. The current index flags are documented in
  handler.h. If you do not implement indexes, just return zero here.

    @details
  part is the key part to check. First key part is 0.
  If all_parts is set, MySQL wants to know the flags for the combined
  index, up to and including 'part'.
*/
ulong crunch::index_flags(uint idx, uint part, bool all_parts) const {
  return 0;
}

ulonglong crunch::table_flags(void) const {
  DBUG_ENTER("crunch::table_flags");
  DBUG_RETURN(HA_REC_NOT_IN_SEQ | HA_CAN_GEOMETRY | HA_TABLE_SCAN_ON_INDEX | HA_CAN_SQL_HANDLER
              | HA_CAN_BIT_FIELD | HA_FILE_BASED | HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE);
};

/** Store lock as requested by mariadb
 *
 * @param thd
 * @param pTHRLockData
 * @param thrLockType
 * @return
 */
THR_LOCK_DATA **crunch::store_lock(THD *thd, THR_LOCK_DATA **pTHRLockData, thr_lock_type thrLockType) {
  if (thrLockType != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type = thrLockType;
  *pTHRLockData++ = &lock;
  return pTHRLockData;
}

/** Store external lock from when LOCK TABLES is called. Currently does nothing.
 *
 * @param thd
 * @param lock_type
 * @return
 */
int crunch::external_lock(THD *thd, int lock_type) {
  DBUG_ENTER("crunch::external_lock");

  int rc = 0;
  crunchTxn *txn = (crunchTxn *) thd_get_ha_data(thd, crunch_hton);
  if (txn == NULL) {
    txn = new crunchTxn(name, dataFolder, transactionDirectory, schemaVersion, indexSchemas);
    thd_set_ha_data(thd, crunch_hton, txn);
  }

  // If we are not unlocking
  if (lock_type != F_UNLCK) {
    txn->tx_isolation = thd->tx_isolation;
    /*if (txn->lock_count == 0) {
      //txn->lock_count= 1;
    }*/

    if (!txn->inProgress) {
      DBUG_PRINT("debug", ("Making new transaction"));
      rc = txn->begin();
      trans_register_ha(thd, thd->in_multi_stmt_transaction_mode(), crunch_hton);
    } else if (txn->inProgress) {
      DBUG_PRINT("debug", ("Using existing transaction %s", txn->uuid.str().c_str()));
      txn->registerNewTable(name, dataFolder, transactionDirectory, schemaVersion, indexSchemas);
    }
  } else {
    if (txn->inProgress) {

      if (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
        /*
          Do like InnoDB: when we get here, it's time to commit a
          single-statement transaction.

          If the statement involved multiple tables, this code will be executed
          for each of them, but that's ok because non-first tx->commit() calls
          will be no-ops.
        */
        txn->tablesInUse--;
        //Check to see if this is last table in transaction
        if (txn->tablesInUse == 0) {
          if (txn->commitOrRollback()) {
            rc = HA_ERR_INTERNAL_ERROR;
          }
          thd_set_ha_data(thd, crunch_hton, NULL);
          delete txn;
        }
      }
    }
  }
  DBUG_RETURN(rc);
}

int crunch::consolidateFiles() {
  DBUG_ENTER("crunch::consolidateFiles");
  int res = 0;
  crunchTxn *txn = new crunchTxn(name, "", transactionDirectory, schemaVersion, indexSchemas);
  int err = 0;
  try {
    rnd_init(true);
    txn->begin();
    while (err != HA_ERR_END_OF_FILE) {
      std::unique_ptr<capnp::MallocMessageBuilder> message = std::make_unique<capnp::MallocMessageBuilder>();
      std::unique_ptr<capnp::FlatArrayMessageReader> rowRead = rnd_row(&err);
      if (rowRead == NULL)
        break;

      uint64_t fileSchemaVersion = dataFiles[dataFileIndex].schemaVersion;
      // If the schema version are not the same, we need to decode and re-encode the row so we can convert the
      // message to the latest schema version
      if (fileSchemaVersion < capnpRowSchema.minimumCompatibleSchemaVersion) {
        schema maxCompatibleSchema = capnpRowSchemas[fileSchemaVersion];
        for (auto schemaIt = capnpRowSchemas.rbegin(); schemaIt != capnpRowSchemas.rend(); ++schemaIt) {
          if (schemaIt->second.minimumCompatibleSchemaVersion <= fileSchemaVersion) {
            maxCompatibleSchema = schemaIt->second;
            break;
          }
        }
        /*
         * Originally the idea was to just call capnpDataToMysqlBuffer but turns out building an array
         * of *Field is not realistically doable. Instead we will using
         * updateMessageToSchema(message, old_schema, new_schema) in order to upgrade a messages content
         */
        message = updateMessageToSchema(std::move(rowRead), maxCompatibleSchema, capnpRowSchema);
      } else {
        capnp::DynamicStruct::Reader messageRoot = rowRead->getRoot<capnp::DynamicStruct>(
            capnpRowSchema.schema);
        message->setRoot(messageRoot);
      }
      write_message(std::move(message), txn);
    }
    res = txn->commitOrRollback();
    rnd_end();
    if (!res) {
      size_t dirlen;
      char dirpath[FN_REFLEN];
      std::string newDataDir = txn->getTransactiondataDirectory(name);
      dirname_part(dirpath, newDataDir.c_str(), &dirlen);
      std::string dataFolderActualPart = newDataDir.substr(dirlen);

      res = my_symlink(dataFolderActualPart.c_str(), (dataFolder + "-tmp").c_str(), MYF(0));
      if (!res) {
        res = my_rename((dataFolder + "-tmp").c_str(), dataFolder.c_str(), MYF(0));
        if (!res) {
          res = removeDirectory(this->name + "/" + this->dataFolderActual);
          this->dataFolderActual = newDataDir;
        } else {
          perror("Error in rename");
        }
      } else {
        perror("Error in symlinking");
      }
    }
  } catch (kj::Exception e) {
    std::cerr << "close exception for table " << name << ": " << e.getFile() << ", line: " << __FILE__ << ":"
              << __LINE__ << ", exception_line: "
              << e.getLine() << ", type: " << (int) e.getType()
              << ", e.what(): " << e.getDescription().cStr() << std::endl;
    res = -331;
  } catch (const std::exception &e) {
    // Log errors
    std::cerr << "close error: " << e.what() << std::endl;
    res = 332;
  }
  delete txn;
  DBUG_RETURN(res);
}

int crunch::findTableFiles(std::string folderName) {
  //Loop through all files in directory of folder and find all files matching extension, add to maps
  std::vector<std::string> files_in_directory = readDirectory(folderName);

  int ret = 0;

  dataFiles.clear();

  for (auto it : files_in_directory) {
    //std::cout << "found file: " << it << " in dir: " << folderName <<std::endl;
    auto extensionIndex = it.find(".");
    if (extensionIndex != std::string::npos) {
      // Handle "./" now that readDirectory returns the leading ./
      if (extensionIndex == 0) {
        auto extensionIndex2 = it.find(".", extensionIndex + 1);
        // Only override if there is more than 1 period
        if (extensionIndex2 != std::string::npos)
          extensionIndex = extensionIndex2;
      }
      std::string extension = it.substr(extensionIndex);
      //std::cout << "found extension: " << extension << " in file: " << it <<std::endl;
      std::smatch schemaMatches, dataMatches, indexSchemaMatches, indexMatches;
      // Must check for TABLE_DATA_EXTENSION first, since a regex for schema will match a substring of the data files
      if (std::regex_search(extension, dataMatches, dataFileExtensionRegex)) {
        try {

          bool fileExists = false;
          uint64_t schema_version = std::stoul(dataMatches[1]);
          std::string newDataFile = it;
          for (auto existingFile : dataFiles) {
            if (existingFile.fileName == newDataFile) {
              fileExists = true;
              break;
            }
          }
          if (!fileExists) {
            data dataStruct = {newDataFile, schema_version};
            dataFiles.push_back(dataStruct);
          }
        } catch (kj::Exception e) {
          std::cerr << "exception on table " << name << ", line: " << __FILE__ << ":"
                    << __LINE__
                    << ", exception_line: " << e.getFile() << ":"
                    << e.getLine() << ", type: " << (int) e.getType()
                    << ", e.what(): " << e.getDescription().cStr() << std::endl;
          return -1010;
        } catch (const std::invalid_argument &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << ", errored with schemaMatches[1]: " << schemaMatches[1]
                    << ", exception: " << e.what() << std::endl;
          return -1011;
        } catch (const std::out_of_range &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << ", errored with schemaMatches[1]: " << schemaMatches[1]
                    << ", exception: " << e.what() << std::endl;
          return -1012;
        } catch (const std::exception &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << "errored when open file with: " << e.what() << std::endl;
          return -1013;
        };
      } else if (std::regex_search(extension, schemaMatches, schemaFileExtensionRegex)) {
        std::string schemaFile = it;

        try {
          uint64_t schema_version = std::stoul(schemaMatches[1]);
          schemaFiles[schema_version] = schemaFile;

          // Parse schema from what was stored during create table
          capnpParsedSchema = parser.parseDiskFile(name, schemaFile, {"/usr/include"});

          capnpParsedSchemas[schema_version] = capnpParsedSchema;

          uint64_t minimumCompatibleSchemaVersion = capnpParsedSchema.getNested(
              "minimumCompatibleSchemaVersion").asConst().as<uint64_t>();

          // Get schema struct name from mysql filepath name
          std::string structName = parseFileNameForStructName(name);
          // Get the nested structure from file, for now there is only a single struct in the schema files

          capnpRowSchema = {capnpParsedSchema.getNested(structName).asStruct(),
                            minimumCompatibleSchemaVersion,
                            schema_version};

          capnpRowSchemas[schema_version] = capnpRowSchema;
        } catch (kj::Exception e) {
          std::cerr << "exception on table " << name << ", line: " << __FILE__ << ":"
                    << __LINE__
                    << ", exception_line: " << e.getFile() << ":"
                    << e.getLine() << ", type: " << (int) e.getType()
                    << ", e.what(): " << e.getDescription().cStr() << std::endl;
          return -1000;
        } catch (const std::invalid_argument &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << ", errored with schemaMatches[1]: " << schemaMatches[1]
                    << ", exception: " << e.what() << std::endl;
          return -1001;
        } catch (const std::out_of_range &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << ", errored with schemaMatches[1]: " << schemaMatches[1]
                    << ", exception: " << e.what() << std::endl;
          return -1002;
        } catch (const std::exception &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << "errored when open file with: " << e.what() << std::endl;
          return -1003;
        };

      } else if (extension == TABLE_DELETE_EXTENSION) {
        //Open crunch delete
        int deleteFileDescriptor;
        deleteFile = it;

        deleteFileDescriptor = my_open(deleteFile.c_str(), mode, 0);
        ret = readDeletesIntoMap(deleteFileDescriptor);
        if (ret)
          return ret;
        if (isFdValid(deleteFileDescriptor)) {
          ret = my_close(deleteFileDescriptor, 0);
          deleteFileDescriptor = 0;
        }
        if (ret)
          return ret;
      } else if (std::regex_search(extension, indexMatches, indexFileExtensionRegex)) {
        try {

          bool fileExists = false;
          uint8_t indexID = std::stoi(indexMatches[1]);
          std::string newIndexFile = it;
          if (indexFiles.find(indexID) != indexFiles.end()) {
            for (auto existingFile : indexFiles[indexID]) {
              if (existingFile.fileName == newIndexFile) {
                fileExists = true;
                break;
              }
            }
          }
          if (!fileExists) {
            indexFile indexStruct = {newIndexFile, // Filename
                                     indexID, // Index ID
                                     getFilesize(newIndexFile.c_str()), // Size
                                     0 // Number of rows
            };
            indexFiles[indexID].push_back(indexStruct);
          }
        } catch (kj::Exception e) {
          std::cerr << "exception on table " << name << ", line: " << __FILE__ << ":"
                    << __LINE__
                    << ", exception_line: " << e.getFile() << ":"
                    << e.getLine() << ", type: " << (int) e.getType()
                    << ", e.what(): " << e.getDescription().cStr() << std::endl;
          return -1030;
        } catch (const std::invalid_argument &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << ", errored with schemaMatches[1]: " << schemaMatches[1]
                    << ", exception: " << e.what() << std::endl;
          return -1031;
        } catch (const std::out_of_range &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << ", errored with schemaMatches[1]: " << schemaMatches[1]
                    << ", exception: " << e.what() << std::endl;
          return -1032;
        } catch (const std::exception &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << "errored when open file with: " << e.what() << std::endl;
          return -1033;
        };
      } else if (std::regex_search(extension, indexSchemaMatches, indexSchemaFileExtensionRegex)) {
        try {
          uint8_t indexID = std::stoi(indexSchemaMatches[1]);
          std::string newIndexSchemaFile = it;
          indexSchemaFiles[indexID] = newIndexSchemaFile;

          // Get schema struct name from mysql filepath name
          std::string structName = parseFileNameForIndexStructName(newIndexSchemaFile);
          // Parse schema from what was stored during create table
          auto capnpIndexParsedSchema = parser.parseDiskFile(structName, newIndexSchemaFile, {"/usr/include"});


          // Get the nested structure from file, for now there is only a single struct in the schema files
          indexSchemas[indexID] = capnpIndexParsedSchema.getNested(structName).asStruct();
        } catch (kj::Exception e) {
          std::cerr << "exception on table " << name << ", line: " << __FILE__ << ":"
                    << __LINE__
                    << ", exception_line: " << e.getFile() << ":"
                    << e.getLine() << ", type: " << (int) e.getType()
                    << ", e.what(): " << e.getDescription().cStr() << std::endl;
          return -1020;
        } catch (const std::invalid_argument &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << ", errored with schemaMatches[1]: " << schemaMatches[1]
                    << ", exception: " << e.what() << std::endl;
          return -1021;
        } catch (const std::out_of_range &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << ", errored with schemaMatches[1]: " << schemaMatches[1]
                    << ", exception: " << e.what() << std::endl;
          return -1022;
        } catch (const std::exception &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << "errored when open file with: " << e.what() << std::endl;
          return -1023;
        };
      }
    }
  }
  return ret;
}

/** Open a table mmap files
 *
 * @param name
 * @param mode
 * @param test_if_locked
 * @return
 */
int crunch::open(const char *name, int mode, uint test_if_locked) {
  DBUG_ENTER("crunch::open");
  int ret = 0;

  if (!(share = get_share()))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock, &lock, NULL);
  options = table->s->option_struct;

#ifndef DBUG_OFF
  DBUG_ASSERT(options);
#endif
  this->mode = mode;
  this->name = name;
  this->dataFolder = this->name + "/" + TABLE_DATA_DIRECTORY;

  this->dataFolderActual = determineSymLink(this->dataFolder);

  ret = findTableFiles(name);
  if (ret)
    DBUG_RETURN(ret);

  // Set the schemaVersion based on the latest we found when opening the table
  schemaVersion = schemaFiles.rbegin()->first;

  dataFileIndex = 0;
  if (dataFiles.size() > 0) {
    data dataStruct = dataFiles[dataFileIndex];
    currentDataFile = dataStruct.fileName;
    capnpRowSchema = capnpRowSchemas.rbegin()->second;
  }

  // Build file names for ondisk
  baseFilePath = name + std::string("/") + table->s->table_name.str;
  transactionDirectory = name + std::string("/") + TABLE_TRANSACTION_DIRECTORY;

  // Catch errors from capnp or libkj
  // TODO handle errors gracefully.


  if (!mmapData(currentDataFile))
    DBUG_RETURN(-1);

  numFields = 0;
  for (Field **field = table->field; *field; field++) {
    numFields++;
  }

  DBUG_RETURN(ret);
}

/** Close table, currently does nothing, will unmmap in the future
 *
 * @return
 */
int crunch::close(void) {
  DBUG_ENTER("crunch::close");
  int res = 0;
  // Close open files
  if (dataFiles.size() > options->consolidation_threshold) {
    consolidateFiles();
  }
  unmmapData();
  DBUG_RETURN(res);
}

/**
 * Create table
 * @param name
 * @param table_arg
 * @param create_info
 * @return
 */
int crunch::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info) {

  char name_buff[FN_REFLEN];
  File create_file;
  options = table_arg->s->option_struct;
  DBUG_ENTER("crunch::create");
  DBUG_PRINT("info", ("Create for table: %s", name));
#ifndef DBUG_OFF
  DBUG_ASSERT(options);
#endif

  schemaVersion = 1;
  this->name = name;

  int err = 0;
  createDirectory(name);
  dataFolder = this->name + "/" + TABLE_DATA_DIRECTORY;
  dataFolderActual = this->name + "/data-init";
  createDirectory(this->dataFolderActual);

  size_t dirlen;
  char dirpath[FN_REFLEN];
  dirname_part(dirpath, this->dataFolderActual.c_str(), &dirlen);
  std::string dataFolderActualPart = this->dataFolderActual.substr(dirlen);

  err = my_symlink(dataFolderActualPart.c_str(), dataFolder.c_str(), MYF(0));
  if (err)
    DBUG_RETURN(err);
  transactionDirectory = name + std::string("/") + TABLE_TRANSACTION_DIRECTORY;
  createDirectory(transactionDirectory);
  // Build capnp proto schema
  std::string capnpSchema = buildCapnpLimitedSchema(
      std::vector<Field *>(table_arg->s->field, table_arg->s->field + table_arg->s->fields),
      parseFileNameForStructName(name), &err, 0, 1,
      1);

  baseFilePath = name + std::string("/") + table_arg->s->table_name.str;
  // Let mysql create the file for us
  if ((create_file = my_create(fn_format(name_buff, (baseFilePath).c_str(), "",
                                         ("." + std::to_string(schemaVersion) + TABLE_SCHEMA_EXTENSION).c_str(),
                                         MY_REPLACE_EXT | MY_UNPACK_FILENAME), 0,
                               O_RDWR | O_TRUNC, MYF(MY_WME))) < 0)
    DBUG_RETURN(-1);

  // Write the capnp schema to schema file
  ::write(create_file, capnpSchema.c_str(), capnpSchema.length());
  my_close(create_file, MYF(0));

  // Create initial data file
  if ((create_file = my_create(fn_format(name_buff, (dataFolder + "/" + table_arg->s->table_name.str).c_str(), "",
                                         ("." + std::to_string(schemaVersion) + TABLE_DATA_EXTENSION).c_str(),
                                         MY_REPLACE_EXT | MY_UNPACK_FILENAME), 0,
                               O_RDWR | O_TRUNC, MYF(MY_WME))) < 0)
    DBUG_RETURN(-1);

  my_close(create_file, MYF(0));

  // Create initial delete file
  if ((create_file = my_create(fn_format(name_buff, baseFilePath.c_str(), "", TABLE_DELETE_EXTENSION,
                                         MY_REPLACE_EXT | MY_UNPACK_FILENAME), 0,
                               O_RDWR, MYF(MY_WME))) < 0)
    DBUG_RETURN(-1);

  my_close(create_file, MYF(0));

  if ((create_file = my_create(fn_format(name_buff, "crunchrowlocation", name,
                                         ".capnp",
                                         MY_REPLACE_EXT | MY_UNPACK_FILENAME), 0,
                               O_RDWR | O_TRUNC, MYF(MY_WME))) < 0)
    DBUG_RETURN(-1);

  // Write the capnp schema to schema file
  ::write(create_file, CRUNCH_ROW_LOCATION_SCHEMA, strlen(CRUNCH_ROW_LOCATION_SCHEMA));
  my_close(create_file, MYF(0));


  // Create index schemas
  DBUG_RETURN(createIndexesFromTable(table_arg));
}

int crunch::delete_table(const char *name) {
  DBUG_ENTER("crunch::delete_table");
  DBUG_PRINT("info", ("Delete for table: %s", name));
  DBUG_RETURN(removeDirectory(name));
}

/**
 *
 * Override default rename table
 *
 * @param from
 * @param to
 * @return
 */
int crunch::rename_table(const char *from, const char *to) {
  DBUG_ENTER("crunch::rename_table");
  DBUG_PRINT("info", ("Rename table from %s to %s", from, to));

  // rename directory
  int ret = my_rename(from, to, MYF(0));
  if (ret) {
    DBUG_PRINT("crunch::rename_table", ("Error: %s", strerror(errno)));
    std::cerr << "error in table " << name << " renaming from " << from
              << " to " << to << ", error: " << strerror(errno) << std::endl;
    DBUG_RETURN(-21);
  }


  // update cap'n proto schema name
  std::string oldSchemaName = parseFileNameForStructName(from);
  std::string newSchemaName = parseFileNameForStructName(to);

  std::vector<std::string> files_in_directory = readDirectory(to);

  // Find all cap'n proto schemas to update
  for (auto it : files_in_directory) {
    auto extensionIndex = it.find(".");
    if (extensionIndex != std::string::npos) {
      // Handle "./" now that readDirectory returns the leading ./
      if (extensionIndex == 0) {
        auto extensionIndex2 = it.find(".", extensionIndex + 1);
        // Only override if there is more than 1 period
        if (extensionIndex2 != std::string::npos)
          extensionIndex = extensionIndex2;
      }
      std::string extension = it.substr(extensionIndex);
      std::smatch schemaMatches;
      // Find all schema files
      if (!std::regex_match(extension, dataFileExtensionRegex) &&
          std::regex_search(extension, schemaMatches, schemaFileExtensionRegex)) {
        std::string schemaFile = it;
        // Rename the existing file
        ret = rename(schemaFile.c_str(), (schemaFile + ".tmp").c_str());
        if (ret) {
          DBUG_PRINT("crunch::rename_table", ("Error: %s", strerror(errno)));
          std::cerr << "error in table " << name << " renaming from " << from
                    << " to " << to << ", error: " << strerror(errno) << std::endl;
          DBUG_RETURN(-22);
        }

        try {
          // Write new schema file with name replaced
          std::ifstream filein(schemaFile + ".tmp");
          std::ofstream fileout(schemaFile);
          if (!filein || !fileout) {
            DBUG_RETURN(-23);
          }

          std::string strTemp;
          while (std::getline(filein, strTemp)) {
            // if the line matches the old schema name remove it
            if (strTemp == ("struct " + oldSchemaName + " {")) {
              strTemp = ("struct " + newSchemaName + " {");
            }
            strTemp += "\n";
            fileout << strTemp;
          }
          // Delete old schema file
          my_delete((schemaFile + ".tmp").c_str(), 0);

        } catch (kj::Exception e) {
          std::cerr << "exception on table " << name << ": " << e.getFile() << ", line: " << __FILE__ << ":"
                    << __LINE__
                    << ", exception_line: "
                    << e.getLine() << ", type: " << (int) e.getType()
                    << ", e.what(): " << e.getDescription().cStr() << std::endl;
          DBUG_RETURN(-24);
        } catch (const std::invalid_argument &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << ", errored with schemaMatches[1]: " << schemaMatches[1]
                    << ", exception: " << e.what() << std::endl;
          DBUG_RETURN(-25);
        } catch (const std::out_of_range &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << ", errored with schemaMatches[1]: " << schemaMatches[1]
                    << ", exception: " << e.what() << std::endl;
          DBUG_RETURN(-26);
        } catch (const std::exception &e) {
          // Log errors
          std::cerr << name << ", line: " << __FILE__ << ":" << __LINE__
                    << "errored when open file with: " << e.what() << std::endl;
          DBUG_RETURN(-27);
        };

      }
    }
  }

  DBUG_RETURN(ret);
}

/**
 * Optimize Table
 * @param thd unused
 * @param check_opt unused
 * @return status of consolidation
 */
int crunch::optimize(THD *thd, HA_CHECK_OPT *check_opt) {
  DBUG_ENTER("crunch::optimize");
  DBUG_RETURN(consolidateFiles());
}

/**
 *
 * @param    altered_table     TABLE object for new version of table.
 * @param    ha_alter_info     Structure describing changes to be done
 *                             by ALTER TABLE and holding data used
 *                             during in-place alter.
 *
 * @retval   HA_ALTER_ERROR                  Unexpected error.
 * @retval   HA_ALTER_INPLACE_NOT_SUPPORTED  Not supported, must use copy.
 * @retval   HA_ALTER_INPLACE_EXCLUSIVE_LOCK Supported, but requires X lock.
 * @retval   HA_ALTER_INPLACE_SHARED_LOCK_AFTER_PREPARE
 *                                           Supported, but requires SNW lock
 *                                           during main phase. Prepare phase
 *                                           requires X lock.
 * @retval   HA_ALTER_INPLACE_SHARED_LOCK    Supported, but requires SNW lock.
 * @retval   HA_ALTER_INPLACE_NO_LOCK_AFTER_PREPARE
 *                                           Supported, concurrent reads/writes
 *                                           allowed. However, prepare phase
 *                                           requires X lock.
 * @retval   HA_ALTER_INPLACE_NO_LOCK        Supported, concurrent
 *                                           reads/writes allowed.
 *
 * @note The default implementation uses the old in-place ALTER API
 * to determine if the storage engine supports in-place ALTER or not.
 *
 * @note Called without holding thr_lock.c lock.
 */
enum_alter_inplace_result
crunch::check_if_supported_inplace_alter(TABLE *altered_table, Alter_inplace_info *ha_alter_info) {
  DBUG_ENTER("crunch::check_if_supported_inplace_alter");
  if (ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_NAME) {
    DBUG_RETURN(enum_alter_inplace_result::HA_ALTER_INPLACE_EXCLUSIVE_LOCK);
  } else if (ha_alter_info->handler_flags & Alter_inplace_info::ADD_STORED_BASE_COLUMN ||
             ha_alter_info->handler_flags & Alter_inplace_info::ALTER_STORED_COLUMN_ORDER ||
             ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_NULLABLE ||
             ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_NOT_NULLABLE ||
             ha_alter_info->handler_flags & Alter_inplace_info::DROP_STORED_COLUMN) {
    DBUG_RETURN(HA_ALTER_INPLACE_NO_LOCK);
  } else if (ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_COLUMN_FORMAT ||
             ha_alter_info->handler_flags & Alter_inplace_info::ALTER_STORED_COLUMN_TYPE ||
             ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_STORAGE_TYPE ||
             ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_EQUAL_PACK_LENGTH) {
    if (checkIfColumnChangeSupportedInplace(altered_table)) {
      DBUG_RETURN(HA_ALTER_INPLACE_NO_LOCK);
    }
    DBUG_PRINT("crunch::check_if_supported_inplace_alter",
               ("%s: Alter in place not supported based on columns that are changing", name.c_str()));
    DBUG_RETURN(enum_alter_inplace_result::HA_ALTER_INPLACE_NOT_SUPPORTED);
  }
  DBUG_RETURN(enum_alter_inplace_result::HA_ALTER_INPLACE_NOT_SUPPORTED);
}

/**
 *
 * @note Storage engines are responsible for reporting any errors by
 * calling my_error()/print_error()
 *
 * @note If this function reports error, commit_inplace_alter_table()
 * will be called with commit= false.
 *
 * @note For partitioning, failing to prepare one partition, means that
 * commit_inplace_alter_table() will be called to roll back changes for
 * all partitions. This means that commit_inplace_alter_table() might be
 * called without prepare_inplace_alter_table() having been called first
 * for a given partition.
 *
 * @param    altered_table     TABLE object for new version of table.
 * @param    ha_alter_info     Structure describing changes to be done
 *                             by ALTER TABLE and holding data used
 *                             during in-place alter.
 *
 * @retval   true              Error
 * @retval   false             Success
 */
bool crunch::prepare_inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info) {
  DBUG_ENTER("crunch::prepare_inplace_alter_table");
  // Build new alter CTX
  crunchInplaceAlterCtx *handlerCtx = new crunchInplaceAlterCtx(name, transactionDirectory, altered_table,
                                                                capnpRowSchemas[schemaVersion].minimumCompatibleSchemaVersion);
  ha_alter_info->handler_ctx = handlerCtx;
  if (ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_NAME) {
    DBUG_RETURN(false);
  } else if (ha_alter_info->handler_flags & Alter_inplace_info::DROP_STORED_COLUMN) {
    DBUG_RETURN(false);
  } else if (ha_alter_info->handler_flags & Alter_inplace_info::ADD_STORED_BASE_COLUMN) {
    std::map<unsigned int, Field *> fieldMap;
    unsigned int newFieldsOffset = 0;
    for (unsigned int i = 0; i < altered_table->s->fields; i++) {
      if (i < table->s->fields && !strcmp(table->field[i]->field_name, altered_table->field[i]->field_name)) {
        fieldMap[i] = altered_table->field[i];
      } else {
        bool foundField = false;
        for (unsigned int j = 0; j < table->s->fields; j++) {
          if (!strcmp(table->field[j]->field_name, altered_table->field[i]->field_name)) {
            fieldMap[j] = altered_table->field[i];
            foundField = true;
            break;
          }
        }
        if (!foundField) {
          fieldMap[table->s->fields + newFieldsOffset] = altered_table->field[i];
          newFieldsOffset++;
        }
      }
    }
    std::vector<Field *> newFields;
    for (auto field : fieldMap) {
      newFields.push_back(field.second);
    }
    handlerCtx->fields = newFields;

    DBUG_RETURN(false);
  } else if (ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_COLUMN_FORMAT ||
             ha_alter_info->handler_flags & Alter_inplace_info::ALTER_STORED_COLUMN_ORDER ||
             ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_NULLABLE ||
             ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_NOT_NULLABLE ||
             ha_alter_info->handler_flags & Alter_inplace_info::ALTER_STORED_COLUMN_TYPE ||
             ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_STORAGE_TYPE ||
             ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_EQUAL_PACK_LENGTH) {
    //Nothing to do here, we know the columns will be in the same order
    DBUG_RETURN(false);
  }
  DBUG_RETURN(true);
}

/**
 *
 *
 * @note Storage engines are responsible for reporting any errors by
 * calling my_error()/print_error()
 *
 * @note If this function reports error, commit_inplace_alter_table()
 * will be called with commit= false.
 *
 * @param    altered_table     TABLE object for new version of table.
 * @param    ha_alter_info     Structure describing changes to be done
 *                             by ALTER TABLE and holding data used
 *                             during in-place alter.
 *
 * @retval   true              Error
 * @retval   false             Success
 */
bool crunch::inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info) {
  DBUG_ENTER("crunch::prepare_inplace_alter_table");
  crunchInplaceAlterCtx *ctx = static_cast<crunchInplaceAlterCtx *>(ha_alter_info->handler_ctx);
  // If we are altering a the schema, build new schema
  if (ha_alter_info->handler_flags & Alter_inplace_info::DROP_STORED_COLUMN ||
      ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_COLUMN_FORMAT ||
      ha_alter_info->handler_flags & Alter_inplace_info::ALTER_STORED_COLUMN_TYPE ||
      ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_STORAGE_TYPE ||
      ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_EQUAL_PACK_LENGTH) {
    DBUG_RETURN(ctx->buildNewCapnpSchema(false));
  }
  if (ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_NAME ||
      ha_alter_info->handler_flags & Alter_inplace_info::ADD_STORED_BASE_COLUMN) {
    DBUG_RETURN(ctx->buildNewCapnpSchema(true));
  }
  if (ha_alter_info->handler_flags & Alter_inplace_info::ALTER_STORED_COLUMN_ORDER ||
      ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_NULLABLE ||
      ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_NOT_NULLABLE)
    DBUG_RETURN(false);
  DBUG_RETURN(true);
}

/**
 *
 * @note Storage engines are responsible for reporting any errors by
 * calling my_error()/print_error()
 *
 * @note If this function with commit= true reports error, it will be called
 *     again with commit= false.
 *
 * @note In case of partitioning, this function might be called for rollback
 *     without prepare_inplace_alter_table() having been called first.
 * Also partitioned tables sets ha_alter_info->group_commit_ctx to a NULL
 * terminated array of the partitions handlers and if all of them are
 * committed as one, then group_commit_ctx should be set to NULL to indicate
 * to the partitioning handler that all partitions handlers are committed.
 * @see prepare_inplace_alter_table().
 *
 * @param    altered_table     TABLE object for new version of table.
 * @param    ha_alter_info     Structure describing changes to be done
 * by ALTER TABLE and holding data used
 * during in-place alter.
 * @param    commit            True => Commit, False => Rollback.
 *
 * @retval   true              Error
 * @retval   false             Success
 */
bool crunch::commit_inplace_alter_table(TABLE *altered_table, Alter_inplace_info *ha_alter_info, bool commit) {
  DBUG_ENTER("crunch::prepare_inplace_alter_table");
  crunchInplaceAlterCtx *ctx = static_cast<crunchInplaceAlterCtx *>(ha_alter_info->handler_ctx);
  if (commit) {
    DBUG_RETURN(ctx->commit());
  } else {
    DBUG_RETURN(ctx->rollback());
  }
  DBUG_RETURN(false);
}

/**
 * Get notified when .frm file is updated, this happens at the end of a alter table
 */
void crunch::notify_table_changed() {
  DBUG_ENTER("crunch::notify_table_changed");
  findTableFiles(name);
  // Set the schemaVersion based on the latest we found when opening the table
  schemaVersion = schemaFiles.rbegin()->first;
  DBUG_VOID_RETURN;
}

int crunch::disconnect(handlerton *hton, MYSQL_THD thd) {
  DBUG_ENTER("crunch::disconnect");
  crunchTxn *txn = (crunchTxn *) thd_get_ha_data(thd, hton);
  delete txn;
  *((crunchTxn **) thd_ha_data(thd, hton)) = 0;
  DBUG_RETURN(0);
}

static int crunch_commit(handlerton *hton, THD *thd, bool all) {
  DBUG_ENTER("crunch_commit");
  int ret = 0;
  crunchTxn *txn = (crunchTxn *) thd_get_ha_data(thd, crunch_hton);

  DBUG_PRINT("debug", ("all: %d", all));
  if (all) {
    if (txn != NULL) {
      ret = txn->commitOrRollback();
      thd_set_ha_data(thd, hton, NULL);
      delete txn;
    }
  }

  if (!ret)
    DBUG_PRINT("info", ("error val: %d", ret));
  DBUG_RETURN(ret);
}


static int crunch_rollback(handlerton *hton, THD *thd, bool all) {
  DBUG_ENTER("crunch_rollback");
  int ret = 0;
  crunchTxn *txn = (crunchTxn *) thd_get_ha_data(thd, crunch_hton);

  DBUG_PRINT("debug", ("all: %d", all));

  if (all) {
    if (txn != NULL) {
      ret = txn->rollback();
      thd_set_ha_data(thd, hton, NULL);
      delete txn;
    }
  }

  if (!ret)
    DBUG_PRINT("info", ("error val: %d", ret));
  DBUG_RETURN(ret);
}

/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each example handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

crunch_share *crunch::get_share() {
  crunch_share *tmp_share;

  DBUG_ENTER("crunch::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share = static_cast<crunch_share *>(get_ha_share_ptr()))) {
    tmp_share = new crunch_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share *>(tmp_share));
  }
  err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
}

/**
 * @brief Check alteredTable structure to see if column changes are supported for inplace alter
 *
 * @param alteredTable
 * @return true on change support
 * @reutrn false if column change type is not supported
 */
bool crunch::checkIfColumnChangeSupportedInplace(TABLE *alteredTable) {
  DBUG_ENTER("crunch::checkIfColumnChangeSupportedInplace");
  for (unsigned int i = 0; i < alteredTable->s->fields; i++) {
    Field *originalField = nullptr;
    Field *alteredField = alteredTable->field[i];
    if (!strcmp(table->field[i]->field_name, alteredField->field_name)) {
      originalField = table->field[i];
    } else {
      for (unsigned int j = 0; j < table->s->fields; j++) {
        if (!strcmp(table->field[j]->field_name, alteredField->field_name)) {
          originalField = table->field[j];
          break;
        }
      }
    }
    // Only check fields that have changed, new fields are supported
    if (originalField != nullptr) {
      DBUG_RETURN(checkIfMysqlColumnTypeCapnpCompatible(originalField, alteredField));
    } else {
      DBUG_PRINT("crunch::checkIfColumnChangeSupportedInplace",
                 ("%s: originalField is nullptr in check if column change is supported online for column %s.", name.c_str(), alteredField->field_name));
    }
  }
  DBUG_RETURN(false);
}

mysql_declare_plugin(crunch)
        {
            MYSQL_STORAGE_ENGINE_PLUGIN,                  /* the plugin type (a MYSQL_XXX_PLUGIN value)   */
            &crunch_storage_engine,                       /* pointer to type-specific plugin descriptor   */
            "Crunch",                                     /* plugin name                                  */
            "Seth Shelnutt",                              /* plugin author (for I_S.PLUGINS)              */
            "Crunch storage engine",                      /* general descriptive text (for I_S.PLUGINS)   */
            PLUGIN_LICENSE_GPL,                           /* the plugin license (PLUGIN_LICENSE_XXX)      */
            crunch_init_func,                             /* Plugin Init */
            NULL,                                         /* Plugin Deinit */
            0x0001,                                       /* version number (0.1) */
            NULL,                                         /* status variables */
            crunch_system_variables,                      /* system variables */
            NULL,                                         /* config options */
            0,                                            /* flags */
        }mysql_declare_plugin_end;
maria_declare_plugin(crunch)
        {
            MYSQL_STORAGE_ENGINE_PLUGIN,                  /* the plugin type (a MYSQL_XXX_PLUGIN value)   */
            &crunch_storage_engine,                       /* pointer to type-specific plugin descriptor   */
            "Crunch",                                     /* plugin name                                  */
            "Seth Shelnutt",                              /* plugin author (for I_S.PLUGINS)              */
            "Crunch storage engine",                      /* general descriptive text (for I_S.PLUGINS)   */
            PLUGIN_LICENSE_GPL,                           /* the plugin license (PLUGIN_LICENSE_XXX)      */
            crunch_init_func,                             /* Plugin Init */
            NULL,                                         /* Plugin Deinit */
            0x0001,                                       /* version number (0.1) */
            NULL,                                         /* status variables */
            crunch_system_variables,                      /* system variables */
            "0.1",                                        /* string version */
            MariaDB_PLUGIN_MATURITY_EXPERIMENTAL          /* maturity */
        }maria_declare_plugin_end;

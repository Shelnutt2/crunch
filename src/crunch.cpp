/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include "crunch.hpp"

#include <iostream>
#include <string>

#include "capnp-mysql.hpp"
#include "utils.hpp"
#include <sys/mman.h>
#include <sql_priv.h>
#include <sql_class.h>

#include "crunch-txn.hpp"

#ifdef UNKNOWN
#undef UNKNOWN
#endif

// Handler for crunch engine
handlerton *crunch_hton;

// crunch file extensions
static const char *crunch_exts[] = {
    TABLE_SCHEME_EXTENSION,
    TABLE_DATA_EXTENSION,
    NullS
};

// Create crunch object
static handler* crunch_create_handler(handlerton *hton,
                                       TABLE_SHARE *table,
                                       MEM_ROOT *mem_root)
{
  return new (mem_root) crunch(hton, table);
}

// Initialization function
static int crunch_init_func(void *p)
{
  DBUG_ENTER("crunch_init_func");

#ifdef HAVE_PSI_INTERFACE
  //init_example_psi_keys();
#endif

  crunch_hton= (handlerton *)p;
  crunch_hton->state = SHOW_OPTION_YES;
  crunch_hton->create = crunch_create_handler;
  crunch_hton->flags = HTON_CAN_RECREATE;
  crunch_hton->tablefile_extensions = crunch_exts;
  crunch_hton->commit = crunch_commit;
  crunch_hton->rollback = crunch_rollback;
  //crunch_hton->close_connection= crunch::disconnect;

  DBUG_RETURN(0);
}

// Storage engine interface
struct st_mysql_storage_engine crunch_storage_engine=
    { MYSQL_HANDLERTON_INTERFACE_VERSION };


bool crunch::mmapData(std::string fileName) {
  DBUG_ENTER("crunch::mmapData");
  currentDataFile = fileName;
  // Get size of data file needed for mmaping
  dataFileSize = getFilesize(fileName.c_str());
  // Only mmap if we have data
  if(isFdValid(dataFileDescriptor)) {
    my_close(dataFileDescriptor,0);
    dataFileDescriptor = 0;
  }
  dataFileDescriptor = my_open(fileName.c_str(), mode, 0);
  if(dataFileSize >0) {
    DBUG_PRINT("crunch::mmap", ("Entering"));
    dataPointer = (capnp::word *)mmap(NULL, dataFileSize, PROT_READ, MAP_SHARED, dataFileDescriptor, 0);

    if ((void *)dataPointer == MAP_FAILED) {
      perror("Error ");
      DBUG_PRINT("crunch::mmap", ("Error: %s", strerror(errno)));
      std::cerr << "mmaped failed for " << fileName <<  " , error: " << strerror(errno) << std::endl;
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
  if (dataFileSize>0 && dataFileStart != NULL && munmap((void*)dataFileStart, dataFileSize) == -1) {
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
  if(oldDataFileSize >0 && dataFileStart != NULL) {
    DBUG_PRINT("crunch::mremap", ("Entering"));

    dataPointer = (capnp::word *)mremap((void*)dataFileStart, oldDataFileSize, dataFileSize, MREMAP_MAYMOVE);
    if ((void *)dataPointer == MAP_FAILED) {
      perror("Error ");
      DBUG_PRINT("crunch::mremap", ("Error: %s", strerror(errno)));
      std::cerr << "mmaped failed for " << currentDataFile <<  " , error: " << strerror(errno) << std::endl;
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
  if (dataFileSize>0 && dataFileStart != NULL && munmap((void*)dataFileStart, dataFileSize) == -1) {
    perror("Error un-mmapping the file");
    DBUG_PRINT("crunch::mremapData", ("Error: %s", strerror(errno)));
    DBUG_RETURN(false);
  }
  DBUG_RETURN(mmapData(fileName));
#endif
}

void crunch::capnpDataToMysqlBuffer(uchar *buf, capnp::DynamicStruct::Reader dynamicStructReader) {

  //Get nulls
  auto nulls = dynamicStructReader.get(NULL_COLUMN_FIELD).as<capnp::DynamicList>();

  // Loop through each field to get the data
  int colNumber = 0;
  std::vector<bool> nullBits;
  for (Field **field=table->field ; *field ; field++) {
    std::string capnpFieldName = camelCase((*field)->field_name);
    auto capnpField = dynamicStructReader.get(capnpFieldName);

    if(!nulls[colNumber].as<bool>()) {
      (*field)->set_notnull();

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
    colNumber++;
  }

}

int crunch::rnd_init(bool scan) {
  DBUG_ENTER("crunch::rnd_init");
  // Lock basic mutex
  mysql_mutex_lock(&share->mutex);
  // Reset row number
  //currentRowNumber = -1;
  // Reset starting mmap position
  findTableFiles(folderName);
  if(currentDataFile != dataFiles[0] || dataPointer == NULL) {
    unmmapData();
    dataFileIndex = 0;
    mmapData(dataFiles[dataFileIndex]);
  }
  dataPointer = dataFileStart;
  dataPointerNext = dataFileStart;
  DBUG_RETURN(0);
}

int crunch::rnd_next(uchar *buf) {
  int rc = 0;
  DBUG_ENTER("crunch::rnd_next");

  // We must set the bitmap for debug purpose, it is "write_set" because we use Field->store
  my_bitmap_map *orig= dbug_tmp_use_all_columns(table, table->write_set);

  //Set datapointer
  dataPointer = dataPointerNext;

  // Before reading we make sure we have not reached the end of the mmap'ed space, which is the end of the file on disk
  if(dataPointer != dataFileStart+(dataFileSize / sizeof(capnp::word))) {
    //Read data
    auto tmpDataMessageReader = std::unique_ptr<capnp::FlatArrayMessageReader>(new capnp::FlatArrayMessageReader(kj::ArrayPtr<const capnp::word>(dataPointer, dataPointer+(dataFileSize / sizeof(capnp::word)))));
    dataPointerNext = tmpDataMessageReader->getEnd();
    uint64_t rowStartLocation = (dataPointer - dataFileStart);
    if(!checkForDeletedRow(currentDataFile, rowStartLocation)) {
      dataMessageReader = std::move(tmpDataMessageReader);
      //currentRowNumber++;

      capnpDataToMysqlBuffer(buf, dataMessageReader->getRoot<capnp::DynamicStruct>(capnpRowSchema));
    } else {
      dbug_tmp_restore_column_map(table->write_set, orig);
      DBUG_RETURN(rnd_next(buf));
    }

  } else { //End of data file
    if (dataFileIndex >= dataFiles.size()-1) {
      rc = HA_ERR_END_OF_FILE;
    } else {
      DBUG_PRINT("info", ("End of file, moving to next"));
      unmmapData();
      dataFileIndex++;
      DBUG_PRINT("info", ("Next file in rnd_next: %s", dataFiles[dataFileIndex].c_str()));
      mmapData(dataFiles[dataFileIndex]);
      dataPointer = dataFileStart;
      dataPointerNext = dataFileStart;
      dbug_tmp_restore_column_map(table->write_set, orig);
      DBUG_RETURN(rnd_next(buf));
    }
  }
  // Reset bitmap to original
  dbug_tmp_restore_column_map(table->write_set, orig);
  DBUG_RETURN(rc);
}

int crunch::rnd_pos(uchar * buf, uchar *pos) {
  int rc = 0;
  DBUG_ENTER("crunch::rnd_pos");

  // We must set the bitmap for debug purpose, it is "write_set" because we use Field->store
  my_bitmap_map *orig = dbug_tmp_use_all_columns(table, table->write_set);

  try {
    uint64_t len;
    memcpy(&len, pos, sizeof(uint64_t));
    kj::ArrayPtr<const unsigned char> bytes = kj::arrayPtr(pos+sizeof(uint64_t), len);

    const kj::ArrayPtr<const capnp::word> view{
      reinterpret_cast<const capnp::word*>(bytes.begin()),
      reinterpret_cast<const capnp::word*>(bytes.end())};

    capnp::FlatArrayMessageReader message(view);
    CrunchRowLocation::Reader rowLocation = message.getRoot<CrunchRowLocation>();
    if (currentDataFile != std::string(rowLocation.getFileName().cStr())) {
      DBUG_PRINT("info", ("rnd_pos is in different file"));
      unmmapData();
      for(unsigned long i = 0; i < dataFiles.size(); i++) {
        if(dataFiles[i] == std::string(rowLocation.getFileName().cStr())) {
          dataFileIndex = i;
          break;
        }
      }
      DBUG_PRINT("info", ("Next file in rnd_next: %s", dataFiles[dataFileIndex].c_str()));
      mmapData(dataFiles[dataFileIndex]);

      dataPointer = dataFileStart;
      dataPointerNext = dataFileStart;
    }
    dataPointer = dataFileStart + rowLocation.getRowStartLocation();
    if (!checkForDeletedRow(rowLocation.getFileName().cStr(), rowLocation.getRowStartLocation())) {
      auto tmpDataMessageReader = std::unique_ptr<capnp::FlatArrayMessageReader>(new capnp::FlatArrayMessageReader(
        kj::ArrayPtr<const capnp::word>(dataPointer, dataPointer + (dataFileSize / sizeof(capnp::word)))));
      dataMessageReader = std::move(tmpDataMessageReader);

      capnpDataToMysqlBuffer(buf, dataMessageReader->getRoot<capnp::DynamicStruct>(capnpRowSchema));
    } else {
      rc = HA_ERR_RECORD_DELETED;
    }
  } catch (kj::Exception e) {
    std::cerr << "exception: " << e.getFile() << ", line: "
              << e.getLine() << ", type: " << (int)e.getType()
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

  int index = 0;
  for (Field **field=table->field ; *field ; field++) {
    std::string capnpFieldName = camelCase((*field)->field_name);
    if ((*field)->is_null()) {
      nulls->set(index++, true);
    } else {
      nulls->set(index++, false);
      switch ((*field)->type()) {

        case MYSQL_TYPE_DOUBLE:{
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
        case MYSQL_TYPE_ENUM:  {
          char attribute_buffer[1024];
          String attribute(attribute_buffer, sizeof(attribute_buffer),
                           &my_charset_bin);
          (*field)->val_str(&attribute, &attribute);

          kj::ArrayPtr<kj::byte> bufferPtr = kj::arrayPtr(attribute.c_ptr_safe(), attribute.length()).asBytes();
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
}

int crunch::write(uchar *buf) {
  // We must set the bitmap for debug purpose, it is "write_set" because we use Field->store
  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);

  crunchTxn *txn= (crunchTxn *)thd_get_ha_data(ha_thd(), crunch_hton);

  // Use a message builder for reach row
  try {
    capnp::MallocMessageBuilder tableRow;

    // Use stored structure
    capnp::DynamicStruct::Builder row = tableRow.initRoot<capnp::DynamicStruct>(capnpRowSchema);

    capnp::DynamicList::Builder nulls = row.init(NULL_COLUMN_FIELD, numFields).as<capnp::DynamicList>();

    build_row(&row, &nulls);

    // Set the fileDescriptor to the end of the file
    lseek(txn->getTransactionDataFileDescriptor(this->name), 0, SEEK_END);
    //Write message to file
    capnp::writeMessageToFd(txn->getTransactionDataFileDescriptor(this->name), tableRow);

  } catch (kj::Exception e) {
    std::cerr << "exception: " << e.getFile() << ", line: "
            << e.getLine() << ", type: " << (int)e.getType()
            << ", e.what(): " << e.getDescription().cStr() << std::endl;
    txn->isTxFailed = true;
    return -321;
  } catch(const std::exception& e) {
    // Log errors
    std::cerr << " write error: " << e.what() << std::endl;
    txn->isTxFailed = true;
    return 321;
  }


  // Reset bitmap to original
  dbug_tmp_restore_column_map(table->read_set, old_map);
  return 0;
}

int crunch::write_row(uchar *buf) {

  DBUG_ENTER("crunch::write_row");
  // Lock basic mutex
  mysql_mutex_lock(&share->mutex);

  int ret = write(buf);
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
  if(ret)
    DBUG_RETURN(ret);
  // If delete was successful, write new row
  ret = write(new_data);
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
    memcpy(ref+sizeof(uint64_t), flatArrayOfLocation.asBytes().begin(), len);
  } catch (kj::Exception e) {
    std::cerr << "exception: " << e.getFile() << ", line: "
              << e.getLine() << ", type: " << (int)e.getType()
              << ", e.what(): " << e.getDescription().cStr() << std::endl;
  };
  DBUG_VOID_RETURN;
}

int crunch::start_stmt(THD *thd, thr_lock_type lock_type)
{
  DBUG_ENTER("crunch::start_stmt");
  int ret= 0;

  crunchTxn *txn= (crunchTxn *)thd_get_ha_data(thd, crunch_hton);
  if (txn == NULL) {
    txn= new crunchTxn(name, transactionDirectory);
    thd_set_ha_data(thd, crunch_hton, txn);
  }

  if (!txn->inProgress) {
    ret = txn->begin();
    //txn->stmt= txn->new_savepoint();
    trans_register_ha(thd, thd->in_multi_stmt_transaction_mode(), crunch_hton);
  } else if (txn->inProgress) {
    txn->registerNewTable(name, transactionDirectory);
  }
  DBUG_RETURN(ret);
}

int crunch::info(uint) {
  DBUG_ENTER("crunch::info");
  /* This is a lie, but you don't want the optimizer to see zero or 1 */
  if (stats.records < 2)
    stats.records= 2;
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

ulonglong crunch::table_flags(void) const{
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
THR_LOCK_DATA** crunch::store_lock(THD* thd, THR_LOCK_DATA** pTHRLockData, thr_lock_type thrLockType) {
  if (thrLockType != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type=thrLockType;
  *pTHRLockData++= &lock;
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
  crunchTxn *txn= (crunchTxn *)thd_get_ha_data(thd, crunch_hton);
  if (txn == NULL) {
    txn= new crunchTxn(name, transactionDirectory);
    thd_set_ha_data(thd, crunch_hton, txn);
  }

  // If we are not unlocking
  if (lock_type != F_UNLCK) {
    txn->tx_isolation= thd->tx_isolation;
    /*if (txn->lock_count == 0) {
      //txn->lock_count= 1;
    }*/

    if (!txn->inProgress) {
      DBUG_PRINT("debug", ("Making new transaction"));
      rc = txn->begin();
      trans_register_ha(thd, thd->in_multi_stmt_transaction_mode(), crunch_hton);
    } else if (txn->inProgress) {
      DBUG_PRINT("debug", ("Using existing transaction %s", txn->uuid.str().c_str()));
      txn->registerNewTable(name, transactionDirectory);
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
        if(txn->tablesInUse == 0) {
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

int crunch::findTableFiles(std::string folderName) {
  //Loop through all files in directory of folder and find all files matching extension, add to maps
  std::vector<std::string> files_in_directory = readDirectory(folderName);

  char name_buff[FN_REFLEN];

  int ret = 0;

  for(auto it : files_in_directory) {
    //std::cout << "found file: " << it << " in dir: " << folderName <<std::endl;
    auto extensionIndex = it.find(".");
    if(extensionIndex != std::string::npos) {
      std::string extension = it.substr(extensionIndex);
      //std::cout << "found extension: " << extension << " in file: " << it <<std::endl;
      if(extension == TABLE_SCHEME_EXTENSION) {
        schemaFile = fn_format(name_buff, it.c_str(), folderName.c_str(),
                  TABLE_SCHEME_EXTENSION,  MY_REPLACE_EXT|MY_UNPACK_FILENAME);
      } else if (extension  == TABLE_DATA_EXTENSION) {
        bool fileExists = false;
        std::string newDataFile = folderName + "/" + it;
        for(auto existingFile : dataFiles) {
          if(existingFile == newDataFile)
            fileExists = true;
        }
        if(!fileExists)
          dataFiles.push_back(newDataFile);
      } else if (extension  == TABLE_DELETE_EXTENSION) {
        //Open crunch delete
        int deleteFileDescriptor;
        deleteFile = fn_format(name_buff, it.c_str(), folderName.c_str(),
                               TABLE_DELETE_EXTENSION, MY_REPLACE_EXT | MY_UNPACK_FILENAME);
        deleteFileDescriptor = my_open(deleteFile.c_str(), mode, 0);
        ret = readDeletesIntoMap(deleteFileDescriptor);
        if (ret)
          return ret;
        if (isFdValid(deleteFileDescriptor)) {
          ret = my_close(deleteFileDescriptor, 0);
          deleteFileDescriptor = 0;
        }
        if(ret)
          return ret;
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
  thr_lock_data_init(&share->lock,&lock,NULL);
#ifndef DBUG_OFF
  ha_table_option_struct *options= table->s->option_struct;

  //DBUG_ASSERT(options);
#endif
  this->mode = mode;
  folderName = name;

  findTableFiles(folderName);

  dataFileIndex = 0;
  currentDataFile = dataFiles[dataFileIndex];

  this->name = name;
  // Build file names for ondisk
  baseFilePath = name + std::string("/") + table->s->table_name.str;
  DBUG_PRINT("info", ("Open for table: %s", baseFilePath.c_str()));
  currentDataFile = baseFilePath +  TABLE_DATA_EXTENSION;

  transactionDirectory = name + std::string("/") + TABLE_TRANSACTION_DIRECTORY;

  /* Delete Any files existing in transaction directory on open,
   * these are incomplete transactions possibly from a crashed session.
   */
  removeDirectory(transactionDirectory);
  createDirectory(transactionDirectory);



  // Catch errors from capnp or libkj
  // TODO handle errors gracefully.
  try {
    // Parse schema from what was stored during create table
    capnpParsedSchema = parser.parseDiskFile(name, schemaFile, {"/usr/include"});
    // Get schema struct name from mysql filepath name
    std::string structName = parseFileNameForStructName(name);
    // Get the nested structure from file, for now there is only a single struct in the schema files
    capnpRowSchema = capnpParsedSchema.getNested(structName).asStruct();

    //my_close(schemaFileDescriptor, 0);
  } catch (kj::Exception e) {
    std::cerr << "exception: " << e.getFile() << ", line: "
              << e.getLine() << ", type: " << (int)e.getType()
              << ", e.what(): " << e.getDescription().cStr() << std::endl;
    close();
    DBUG_RETURN(-2);
  } catch(const std::exception& e) {
    // Log errors
    std::cerr << name << " errored when open file with: " << e.what() << std::endl;
    close();
    DBUG_RETURN(2);
  };

  if(!mmapData(currentDataFile))
    DBUG_RETURN(-1);

  numFields = 0;
  for (Field **field=table->field ; *field ; field++) {
    numFields++;
  }

  DBUG_RETURN(ret);
}

/** Close table, currently does nothing, will unmmap in the future
 *
 * @return
 */
int crunch::close(void){
  DBUG_ENTER("crunch::close");
  // Close open files
  if (dataFileStart != NULL && munmap((void*)dataFileStart, dataFileSize) == -1) {
    perror("Error un-mmapping the file");
    DBUG_PRINT("crunch::close", ("Error: %s", strerror(errno)));
  }
  unmmapData();
  DBUG_RETURN(0);
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
  ha_table_option_struct *options= table_arg->s->option_struct;
  DBUG_ENTER("crunch::create");
  DBUG_PRINT("info", ("Create for table: %s", name));
//DBUG_ASSERT(options);

  int err = 0;
  std::string tableName = table_arg->s->table_name.str;
  createDirectory(name);
  transactionDirectory = name + std::string("/") + TABLE_TRANSACTION_DIRECTORY;
  createDirectory(transactionDirectory);
  // Cap'n Proto schema's require the first character to be upper case for struct names
  tableName[0] = toupper(tableName[0]);
  // Build capnp proto schema
  std::string capnpSchema = buildCapnpLimitedSchema(table_arg->s->field, tableName, &err);

  baseFilePath = name + std::string("/") + table_arg->s->table_name.str;
  folderName = name;
  // Let mysql create the file for us
  if ((create_file= my_create(fn_format(name_buff, baseFilePath.c_str(), "", TABLE_SCHEME_EXTENSION,
                                        MY_REPLACE_EXT|MY_UNPACK_FILENAME),0,
                              O_RDWR | O_TRUNC,MYF(MY_WME))) < 0)
    DBUG_RETURN(-1);

  // Write the capnp schema to schema file
  ::write(create_file, capnpSchema.c_str(), capnpSchema.length());
  my_close(create_file,MYF(0));

  // Create initial data file
  if ((create_file= my_create(fn_format(name_buff, baseFilePath.c_str(), "", TABLE_DATA_EXTENSION,
                                        MY_REPLACE_EXT|MY_UNPACK_FILENAME),0,
                              O_RDWR | O_TRUNC,MYF(MY_WME))) < 0)
    DBUG_RETURN(-1);

  my_close(create_file,MYF(0));

  // Create initial delete file
  if ((create_file = my_create(fn_format(name_buff, baseFilePath.c_str(), "", TABLE_DELETE_EXTENSION,
                                        MY_REPLACE_EXT | MY_UNPACK_FILENAME), 0,
                              O_RDWR, MYF(MY_WME))) < 0)
    DBUG_RETURN(-1);

  my_close(create_file,MYF(0));


  DBUG_RETURN(0);
}

int crunch::delete_table(const char *name)
{
  DBUG_ENTER("crunch::delete_table");
  DBUG_PRINT("info", ("Delete for table: %s", name));
  removeDirectory(name);
  DBUG_RETURN(0);
}

int crunch::disconnect(handlerton *hton, MYSQL_THD thd)
{
  DBUG_ENTER("crunch::disconnect");
  crunchTxn *txn= (crunchTxn *) thd_get_ha_data(thd, hton);
  delete txn;
  *((crunchTxn **) thd_ha_data(thd, hton))= 0;
  DBUG_RETURN(0);
}

static int crunch_commit(handlerton *hton, THD *thd, bool all)
{
  DBUG_ENTER("crunch_commit");
  int ret= 0;
  crunchTxn *txn= (crunchTxn *)thd_get_ha_data(thd, crunch_hton);

  DBUG_PRINT("debug", ("all: %d", all));
  if (all)
  {
    if(txn != NULL) {
      ret = txn->commitOrRollback();
      thd_set_ha_data(thd, hton, NULL);
      delete txn;
    }
  }

  if(!ret)
    DBUG_PRINT("info", ("error val: %d", ret));
  DBUG_RETURN(ret);
}


static int crunch_rollback(handlerton *hton, THD *thd, bool all)
{
  DBUG_ENTER("crunch_rollback");
  int ret= 0;
  crunchTxn *txn= (crunchTxn *)thd_get_ha_data(thd, crunch_hton);

  DBUG_PRINT("debug", ("all: %d", all));

  if (all)
  {
    if(txn != NULL) {
      ret = txn->rollback();
      thd_set_ha_data(thd, hton, NULL);
      delete txn;
    }
  }

  if(!ret)
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

crunch_share* crunch::get_share()
{
  crunch_share* tmp_share;

  DBUG_ENTER("crunch::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share= static_cast<crunch_share*>(get_ha_share_ptr())))
  {
    tmp_share= new crunch_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share*>(tmp_share));
  }
  err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
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
            NULL,                                         /* system variables */
            NULL,                                         /* config options */
            0,                                            /* flags */
        }
    mysql_declare_plugin_end;
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
            NULL,                                         /* system variables */
            "0.1",                                        /* string version */
            MariaDB_PLUGIN_MATURITY_EXPERIMENTAL          /* maturity */
        }
    maria_declare_plugin_end;

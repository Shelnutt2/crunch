/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include "crunch.hpp"

#include <iostream>
#include <string>

#include "capnp-mysql.hpp"
#include "utils.hpp"
#include <sys/mman.h>

#include <capnp/serialize.h>

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

  DBUG_RETURN(0);
}

// Storage engine interface
struct st_mysql_storage_engine crunch_storage_engine=
    { MYSQL_HANDLERTON_INTERFACE_VERSION };


bool crunch::mmapData() {
  // Get size of data file needed for mmaping
  dataFileSize = getFilesize(dataFile.c_str());
  // Only mmap if we have data
  if(dataFileSize >0) {
    DBUG_PRINT("crunch::mmap", ("Entering"));
    dataPointer = (capnp::word *)mmap(NULL, dataFileSize, PROT_READ, MAP_SHARED, dataFileDescriptor, 0);

    if ((void *)dataPointer == MAP_FAILED) {
      perror("Error ");
      DBUG_PRINT("crunch::mmap", ("Error: %s", strerror(errno)));
      std::cerr << "mmaped failed for " << dataFile <<  " , error: " << strerror(errno) << std::endl;
      my_close(dataFileDescriptor, 0);
      return false;
    }

    // Set the start pointer to the current dataPointer
    dataFileStart = dataPointer;

    // get size of a row
    sizeOfSingleRow = (dataFileStart - capnp::FlatArrayMessageReader(kj::ArrayPtr<const capnp::word>(dataPointer, dataPointer+(dataFileSize / sizeof(capnp::word)))).getEnd()) / sizeof(capnp::word);
  } else {
    dataPointer = dataFileStart = NULL;
  }
  return true;
}

bool crunch::mremapData() {
#ifdef __linux // Only linux support mremap call
  int oldDataFileSize = dataFileSize;
  // Get size of data file needed for mremaping
  dataFileSize = getFilesize(dataFile.c_str());
  // Only mmap if we have data
  if(oldDataFileSize >0 && dataFileStart != NULL) {
    DBUG_PRINT("crunch::mremap", ("Entering"));

    dataPointer = (capnp::word *)mremap((void*)dataFileStart, oldDataFileSize, dataFileSize, MREMAP_MAYMOVE);
    if ((void *)dataPointer == MAP_FAILED) {
      perror("Error ");
      DBUG_PRINT("crunch::mremap", ("Error: %s", strerror(errno)));
      std::cerr << "mmaped failed for " << dataFile <<  " , error: " << strerror(errno) << std::endl;
      my_close(dataFileDescriptor, 0);
      return false;
    }

    // Set the start pointer to the current dataPointer
    dataFileStart = dataPointer;

    // get size of a row
    sizeOfSingleRow = (dataFileStart - capnp::FlatArrayMessageReader(kj::ArrayPtr<const capnp::word>(dataPointer, dataPointer+(dataFileSize / sizeof(capnp::word)))).getEnd()) / sizeof(capnp::word);
  } else {
    dataPointer = dataFileStart = NULL;
    return mmapData();
  }
  return true;
#else
  if (dataFileSize>0 && dataFileStart != NULL && munmap((void*)dataFileStart, dataFileSize) == -1) {
    perror("Error un-mmapping the file");
    DBUG_PRINT("crunch::mremapData", ("Error: %s", strerror(errno)));
    return false;
  }
  return mmapData();
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
          (*field)->store(capnpField.as<int64_t>(), 0);
          break;
        case capnp::DynamicValue::UINT:
          (*field)->store(capnpField.as<uint64_t>());
          break;
        case capnp::DynamicValue::FLOAT:
          (*field)->store(capnpField.as<double>());
          break;
        case capnp::DynamicValue::TEXT: {
          const char *row_string = capnpField.as<capnp::Text>().cStr();
          (*field)->store(row_string, strlen(row_string), &my_charset_utf8_general_ci);
          break;
        }
        case capnp::DynamicValue::DATA:
        case capnp::DynamicValue::LIST:
        case capnp::DynamicValue::ENUM:
        case capnp::DynamicValue::STRUCT:
        case capnp::DynamicValue::CAPABILITY:
        case capnp::DynamicValue::ANY_POINTER:
        case capnp::DynamicValue::UNKNOWN:
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
  currentRowNumber = 0;
  // Reset starting mmap position
  dataPointer = dataFileStart;
  DBUG_RETURN(0);
}
int crunch::rnd_next(uchar *buf) {
  int rc = 0;
  DBUG_ENTER("crunch::rnd_next");

  // We must set the bitmap for debug purpose, it is "write_set" because we use Field->store
  my_bitmap_map *orig= dbug_tmp_use_all_columns(table, table->write_set);

  // Before reading we make sure we have not reached the end of the mmap'ed space, which is the end of the file on disk
  if(dataPointer != dataFileStart+(dataFileSize / sizeof(capnp::word))) {
    //Read data
    dataMessageReader = std::unique_ptr<capnp::FlatArrayMessageReader>(new capnp::FlatArrayMessageReader(kj::ArrayPtr<const capnp::word>(dataPointer, dataPointer+(dataFileSize / sizeof(capnp::word)))));
    dataPointer = dataMessageReader->getEnd();
    currentRowNumber++;

    capnpDataToMysqlBuffer(buf, dataMessageReader->getRoot<capnp::DynamicStruct>(capnpRowSchema));

  } else { //End of data file
    rc = HA_ERR_END_OF_FILE;
  }
  // Reset bitmap to original
  dbug_tmp_restore_column_map(table->write_set, orig);
  DBUG_RETURN(rc);
}

int crunch::rnd_pos(uchar * buf, uchar *pos) {
  int rc = 0;
  DBUG_ENTER("crunch::rnd_pos");
  rc= HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}

int crunch::rnd_end() {
  DBUG_ENTER("crunch::rnd_end");
  // Unlock basic mutex
  mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(0);
}

int crunch::write_row(uchar *buf) {

  DBUG_ENTER("crunch::write_row");
  // Lock basic mutex
  mysql_mutex_lock(&share->mutex);
  // We must set the bitmap for debug purpose, it is "write_set" because we use Field->store
  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);

  // Use a message builder for reach row
  capnp::MallocMessageBuilder tableRow;

  // Use stored structure
  capnp::DynamicStruct::Builder row = tableRow.initRoot<capnp::DynamicStruct>(capnpRowSchema);

  capnp::DynamicList::Builder nulls =  row.init(NULL_COLUMN_FIELD, numFields).as<capnp::DynamicList>();

  // Loop through each field to write row

  int index = 0;
  for (Field **field=table->field ; *field ; field++) {
    std::string capnpFieldName = camelCase((*field)->field_name);
    if ((*field)->is_null()) {
      nulls.set(index++, true);
    } else {
      nulls.set(index++, false);
      switch ((*field)->type()) {

        case MYSQL_TYPE_DOUBLE:{
          row.set(capnpFieldName, (*field)->val_real());
          break;
        }
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL: {
          row.set(capnpFieldName, (*field)->val_real());
          break;
        }

        case MYSQL_TYPE_FLOAT: {
          row.set(capnpFieldName, (*field)->val_real());
          break;
        }

        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_YEAR:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG: {
          row.set(capnpFieldName, (*field)->val_int());
          break;
        }

        case MYSQL_TYPE_NULL: {
          row.set(capnpFieldName, capnp::DynamicValue::VOID);
          break;
        }

        case MYSQL_TYPE_BIT: {
          row.set(capnpFieldName, (*field)->val_bool());
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
          row.set(capnpFieldName, attribute.c_ptr());
          break;
        }

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
        case MYSQL_TYPE_NEWDATE: {
          char attribute_buffer[1024];
          String attribute(attribute_buffer, sizeof(attribute_buffer),
                            &my_charset_bin);
          (*field)->val_str(&attribute);
          capnp::DynamicValue::Reader r(attribute.ptr());
          row.set(capnpFieldName, r);
          break;
        }
      }
    }
  }

  // Set the fileDescriptor to the end of the file
  lseek(dataFileDescriptor, 0, SEEK_END);
  //Write message to file
  capnp::writeMessageToFd(dataFileDescriptor, tableRow);

  // mremap the datafile since it's grown. This is a naive approach, ideally we'd like to do this from a fswatch thread and after a transaction completes
  if(!mremapData()) {
    std::cerr << "Could not mremap data file after writing row" << std::endl;
    DBUG_RETURN(-1);
  }


  // Reset bitmap to original
  dbug_tmp_restore_column_map(table->read_set, old_map);

  // Unlock basic mutex
  mysql_mutex_unlock(&share->mutex);
  DBUG_RETURN(0);
}

void crunch::position(const uchar *record) {
  DBUG_ENTER("crunch::position");
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
  // TODO: Look into HA_REC_NOT_IN_SEQ
  DBUG_RETURN(HA_NO_TRANSACTIONS | HA_TABLE_SCAN_ON_INDEX | HA_NO_BLOBS | HA_CAN_SQL_HANDLER
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
  DBUG_RETURN(0);
}

/** Open a table, currently does nothing, will mmap files in the future
 *
 * @param name
 * @param mode
 * @param test_if_locked
 * @return
 */
int crunch::open(const char *name, int mode, uint test_if_locked) {
  DBUG_ENTER("crunch::open");

  if (!(share = get_share()))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock,&lock,NULL);
#ifndef DBUG_OFF
  ha_table_option_struct *options= table->s->option_struct;

  //DBUG_ASSERT(options);
#endif

  // Build file names for ondisk
  tableName = name;
  schemaFile = tableName +  TABLE_SCHEME_EXTENSION;
  dataFile = tableName +  TABLE_DATA_EXTENSION;
  schemaFileDescriptor = my_open(schemaFile.c_str(), mode, 0);
  dataFileDescriptor = my_open(dataFile.c_str(), mode, 0);


  // Catch errors from capnp or libkj
  // TODO handle errors gracefully.
  try {
    // Parse schema from what was stored during create table
    capnpParsedSchema = parser.parseDiskFile(name, schemaFile, {"/usr/include"});
    // Get schema struct name from mysql filepath name
    std::string structName = parseFileNameForStructName(name);
    // Get the nested structure from file, for now there is only a single struct in the schema files
    capnpRowSchema = capnpParsedSchema.getNested(structName).asStruct();
  } catch(const std::exception& e) {
    // Log errors
    std::cerr << name << " errored when open file with: " << e.what() << std::endl;
    close();
    DBUG_RETURN(2);
  };

  if(!mmapData())
    DBUG_RETURN(-1);

  numFields = 0;
  for (Field **field=table->field ; *field ; field++) {
    numFields++;
  }
  DBUG_RETURN(0);
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
  my_close(schemaFileDescriptor, 0);
  my_close(dataFileDescriptor, 0);
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
  // Cap'n Proto schema's require the first character to be upper case for struct names
  tableName[0] = toupper(tableName[0]);
  // Build capnp proto schema
  std::string capnpSchema = buildCapnpLimitedSchema(table_arg->s->field, tableName, &err);

  // Let mysql create the file for us
  if ((create_file= my_create(fn_format(name_buff, name, "", TABLE_SCHEME_EXTENSION,
                                        MY_REPLACE_EXT|MY_UNPACK_FILENAME),0,
                              O_RDWR | O_TRUNC,MYF(MY_WME))) < 0)
    DBUG_RETURN(-1);

  // Write the capnp schema to schema file
  write(create_file, capnpSchema.c_str(), capnpSchema.length());
  my_close(create_file,MYF(0));

  // Create initial data file
  if ((create_file= my_create(fn_format(name_buff, name, "", TABLE_DATA_EXTENSION,
                                        MY_REPLACE_EXT|MY_UNPACK_FILENAME),0,
                              O_RDWR | O_TRUNC,MYF(MY_WME))) < 0)
    DBUG_RETURN(-1);

  my_close(create_file,MYF(0));

  DBUG_RETURN(0);
}

/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each example handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

std::unique_ptr<crunch_share> crunch::get_share()
{
  std::unique_ptr<crunch_share> tmp_share;

  DBUG_ENTER("crunch::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share= std::unique_ptr<crunch_share>(static_cast<crunch_share*>(get_ha_share_ptr()))))
  {
    tmp_share= std::unique_ptr<crunch_share>(new crunch_share);
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share*>(tmp_share.get()));
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
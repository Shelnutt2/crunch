/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include "crunch.hpp"

#include <table.h>
#include <field.h>
#include <iostream>

#include "capnp-mysql.hpp"
#include "utils.hpp"

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


int crunch::rnd_init(bool scan) {
  DBUG_ENTER("crunch::rnd_init");
  DBUG_RETURN(0);
}
int crunch::rnd_next(uchar *buf) {
  int rc;
  DBUG_ENTER("crunch::rnd_next");
  rc= HA_ERR_END_OF_FILE;
  DBUG_RETURN(rc);
}

int crunch::rnd_pos(uchar * buf, uchar *pos) {
  int rc;
  DBUG_ENTER("crunch::rnd_pos");
  rc= HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}

int crunch::rnd_end() {
  DBUG_ENTER("crunch::rnd_end");
  DBUG_RETURN(0);
}

void crunch::position(const uchar *record) {
  DBUG_ENTER("crunch::position");
}

int crunch::info(uint) {
  DBUG_ENTER("crunch::info");
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
  DBUG_RETURN(0);
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

  std::string tableName = name;
  std::string schemaFile = tableName +  TABLE_SCHEME_EXTENSION;
  std::string dataFile = tableName +  TABLE_DATA_EXTENSION;
  schemaFileDescriptor = my_open(schemaFile.c_str(), mode, 0);
  dataFileDescriptor = my_open(dataFile.c_str(), mode, 0);

  try {
    capnpParsedSchema = parser.parseDiskFile(name, schemaFile, {"/usr/include"});
    std::string structName = parseFileNameForStructName(name);
    capnpRowSchema = capnpParsedSchema.getNested(structName).asStruct();
  } catch(const std::exception& e) {
    std::cerr << name << " errored when open file with: " << e.what() << std::endl;
    close();
    DBUG_RETURN(-1);
  };

  DBUG_RETURN(0);
}

/** Close table, currently does nothing, will unmmap in the future
 *
 * @return
 */
int crunch::close(void){
  DBUG_ENTER("crunch::close");
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

  for (Field **field= table_arg->s->field; *field; field++)
  {
    ha_field_option_struct *field_options= (*field)->option_struct;
    //DBUG_ASSERT(field_options);
    DBUG_PRINT("info", ("field: %s",
        (*field)->field_name));
  }
  int err = 0;
  std::string tableName = table_arg->s->table_name.str;
  tableName[0] = toupper(tableName[0]);
  std::string capnpSchema = buildCapnpLimitedSchema(table_arg->s->field, tableName, &err);

  if ((create_file= my_create(fn_format(name_buff, name, "", TABLE_SCHEME_EXTENSION,
                                        MY_REPLACE_EXT|MY_UNPACK_FILENAME),0,
                              O_RDWR | O_TRUNC,MYF(MY_WME))) < 0)
    DBUG_RETURN(-1);

  write(create_file, capnpSchema.c_str(), capnpSchema.length());
  my_close(create_file,MYF(0));

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
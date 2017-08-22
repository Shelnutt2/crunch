/*
** Licensed under the GNU Lesser General Public License v3 or later
*/


#ifndef CRUNCH_CRUNCH_HPP
#define CRUNCH_CRUNCH_HPP

#include <my_global.h>           /* ulonglong */
#include <thr_lock.h>            /* THR_LOCK, THR_LOCK_DATA */
#include <handler.h>             /* handler */
#include <table.h>               /* table */
#include <field.h>               /* field */
#include <my_base.h>             /* ha_rows */
#include <memory>                /* unique_ptr */
#include <cstdint>               /* uint64_t */
#include <unordered_map> /*Unordered map*/

#include <capnp/schema.h>        /* Cap'n Proto Schema */
#include <capnp/schema-parser.h> /* Cap'n Proto SchemaParser */
#include <capnp/message.h>       /* Cap'n Proto Message */
#include <capnp/serialize.h>     /* Cap'n Proto FlatArrayMessageReader */
#include <capnp/dynamic.h>     /* Cap'n Proto DynamicStruct::Reader */

#include "crunchdeleterow.capnp.h"

#define TABLE_SCHEME_EXTENSION ".capnp"
#define TABLE_DATA_EXTENSION ".capnpd"
#define TABLE_DELETE_EXTENSION ".deleted.capnpd"

// TODO: Figure out if this is needed, or can we void the performance schema for now?
static PSI_mutex_key ex_key_mutex_Example_share_mutex;

/**
  Structure for CREATE TABLE options (table options).
  It needs to be called ha_table_option_struct.

  The option values can be specified in the CREATE TABLE at the end:
  CREATE TABLE ( ... ) *here*
*/

/** @brief
 Crunch_share is a class that will be shared among all open handlers.
*/
class crunch_share : public Handler_share {
public:
    mysql_mutex_t mutex;
    THR_LOCK lock;
    crunch_share(){
      thr_lock_init(&lock);
      mysql_mutex_init(ex_key_mutex_Example_share_mutex,
                       &mutex, MY_MUTEX_INIT_FAST);
    };
    ~crunch_share()
    {
      thr_lock_delete(&lock);
      mysql_mutex_destroy(&mutex);
    }
};

class crunch : public handler {
  public:
    crunch(handlerton *hton, TABLE_SHARE *table_arg):handler(hton, table_arg){};
    ~crunch() noexcept(true){};
    int rnd_init(bool scan);
    int rnd_next(uchar *buf);
    int rnd_pos(uchar * buf, uchar *pos);
    int rnd_end();
    int write_row(uchar *buf);
    int delete_row(const uchar *buf);
    void position(const uchar *record);
    int info(uint);
    ulong index_flags(uint idx, uint part, bool all_parts) const;
    THR_LOCK_DATA** store_lock(THD* thd, THR_LOCK_DATA** pTHRLockData, thr_lock_type thrLockType);
    int external_lock(THD *thd, int lock_type);
    int open(const char *name, int mode, uint test_if_locked);
    int close(void);
    ulonglong table_flags(void) const;
    int create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info);
    int delete_table(const char *name);
    int readDeletesIntoMap(FILE* deleteFilePointer);
    bool checkForDeletedRow(std::string fileName, uint64_t rowStartLocation);
    void markRowAsDeleted(std::string fileName, uint64_t rowStartLocation, uint64_t rowEndLocation);

    static inline bool
    row_is_fixed_length(TABLE *table)
    {
      return table->s->blob_fields + table->s->varchar_fields == 0;
    }

    static inline void
    extract_varchar_field_info(Field *field, uint32_t *len_bytes,
                               uint32_t *field_size, uint8_t *src)
    {
      // see Field_blob::Field_blob() (in field.h) - need 1-4 bytes to
      // store the real size
      if (likely(field->field_length <= 255)) {
        *field_size = *src;
        *len_bytes = 1;
        return;
      }
      if (likely(field->field_length <= 65535)) {
        *field_size = *(uint16_t *)src;
        *len_bytes = 2;
        return;
      }
      if (likely(field->field_length <= 16777215)) {
        *field_size = (src[2] << 16) | (src[1] << 8) | (src[0] << 0);
        *len_bytes = 3;
        return;
      }
      *field_size = *(uint32_t *)src;
      *len_bytes = 4;
    }

private:

    void capnpDataToMysqlBuffer(uchar *buf, capnp::DynamicStruct::Reader  dynamicStructReader);

    bool mmapData();
    bool mremapData();

    THR_LOCK_DATA lock;      ///< MySQL lock
    crunch_share* share;    ///< Shared lock info
    crunch_share* get_share(); ///< Get the share

    ::capnp::ParsedSchema capnpParsedSchema;
    ::capnp::StructSchema capnpRowSchema;
    ::capnp::SchemaParser parser;

    std::string baseFilePath;
    std::string folderName;
    std::string schemaFile;
    std::string dataFile;
    std::string deleteFile;
    int schemaFileDescriptor;
    int dataFileDescriptor;
    FILE* deleteFilePointer;
    //int deleteFileDescriptor;
    int dataFileSize;

    std::unique_ptr<capnp::FlatArrayMessageReader> dataMessageReader; // Last capnp message read from data file

    // Position variables
    int currentRowNumber;
    int records;
    int numFields;
    const capnp::word *dataPointer;
    const capnp::word *dataPointerNext;
    const capnp::word *dataFileStart;

    std::unordered_map<std::string, std::shared_ptr<std::unordered_map<uint64_t,CrunchDeleteRow::Reader>>> deleteMap;
};


#endif //CRUNCH_CRUNCH_HPP

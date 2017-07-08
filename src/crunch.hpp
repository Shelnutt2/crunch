/*
** Licensed under the GNU Lesser General Public License v3 or later
*/


#ifndef CRUNCH_CRUNCH_HPP
#define CRUNCH_CRUNCH_HPP

#include <my_global.h>                  /* ulonglong */
#include <private/thr_lock.h>           /* THR_LOCK, THR_LOCK_DATA */
#include <private/handler.h>            /* handler */
#include <private/my_base.h>            /* ha_rows */
#include <memory>                       /* unique_ptr */

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

    int rnd_init(bool scan);
    int rnd_next(uchar *buf);
    int rnd_pos(uchar * buf, uchar *pos);
    int rnd_end();
    void position(const uchar *record);
    int info(uint);
    ulong index_flags(uint idx, uint part, bool all_parts) const;
    THR_LOCK_DATA** store_lock(THD* thd, THR_LOCK_DATA** pTHRLockData, thr_lock_type thrLockType);
    int external_lock(THD *thd, int lock_type);
    int open(const char *name, int mode, uint test_if_locked);
    int close(void);
    ulonglong table_flags(void) const;
    int create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info);

private:
    THR_LOCK_DATA lock;      ///< MySQL lock
    std::unique_ptr<crunch_share> share;    ///< Shared lock info
    std::unique_ptr<crunch_share> get_share(); ///< Get the share
};


#endif //CRUNCH_CRUNCH_HPP

/*
** Created by Seth Shelnutt on 3/4/18.
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include "crunch.hpp"
#include "utils.hpp"
#include "capnp-mysql.hpp"

int crunch::createIndexesFromTable(TABLE *table_arg) {
  DBUG_ENTER("crunch::createIndexFromTable");
  int rc = 0;
  char name_buff[FN_REFLEN];

  for(uint i = 0; i < table_arg->s->keys; i++) {
    KEY key = table_arg->key_info[i];
    std::string indexSchema = buildCapnpIndexSchema(&key, "", &rc, 0);
    File create_file;
    // Let mysql create the file for us
    if ((create_file = my_create(fn_format(name_buff, (baseFilePath + "-" + key.name).c_str(), "",
                                           TABLE_INDEX_SCHEME_EXTENSION,
                                           MY_REPLACE_EXT | MY_UNPACK_FILENAME), 0,
                                 O_RDWR | O_TRUNC, MYF(MY_WME))) < 0)
      DBUG_RETURN(-1);

    // Write the capnp schema to schema file
    ::write(create_file, indexSchema.c_str(), indexSchema.length());
    my_close(create_file, MYF(0));
  }
  DBUG_RETURN(rc);
}
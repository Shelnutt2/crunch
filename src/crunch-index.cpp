/*
** Created by Seth Shelnutt on 3/4/18.
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include "crunch.hpp"
#include "utils.hpp"

/**
 * @brief Function to read all indexes and create schema files
 *
 * @param table_arg to read keys from
 * @return status code
 */
int crunch::createIndexesFromTable(TABLE *table_arg) {
  DBUG_ENTER("crunch::createIndexFromTable");
  int rc = 0;
  char name_buff[FN_REFLEN];

  for (uint i = 0; i < table_arg->s->keys; i++) {
    KEY key = table_arg->key_info[i];
    // Build capnp schema for given key
    std::string indexSchema = buildCapnpIndexSchema(&key, parseFileNameForStructName(key.name), &rc, 0);
    File create_file;
    // Create index schema file
    if ((create_file = my_create(fn_format(name_buff, (baseFilePath + "-" + key.name).c_str(), "",
                                           ("." + std::to_string(i) + TABLE_INDEX_SCHEMA_EXTENSION).c_str(),
                                           MY_REPLACE_EXT | MY_UNPACK_FILENAME), 0,
                                 O_RDWR | O_TRUNC, MYF(MY_WME))) < 0)
      DBUG_RETURN(-1);

    // Write the capnp index schema to file
    ::write(create_file, indexSchema.c_str(), indexSchema.length());
    my_close(create_file, MYF(0));
  }
  DBUG_RETURN(rc);
}

/**
 *
 * @brief create index messages and writes to file based on tableRow
 *
 * @param tableRow
 * @param txn
 * @return status code
 */
int crunch::build_and_write_indexes(std::shared_ptr<capnp::MallocMessageBuilder> tableRow, crunchTxn *txn) {

  int rc = 0;
  for (auto index : indexSchemas) {
    std::unique_ptr<capnp::MallocMessageBuilder> indexRow = build_index(tableRow, index.first);
    rc = write_index(std::move(indexRow), txn, index.first);
    if (!rc)
      return rc;
  }

  return rc;
}

/**
 *
 * @brief
 *
 * @param tableRow
 * @param txn
 * @return status code
 */
int crunch::write_index(std::unique_ptr<capnp::MallocMessageBuilder> indexRow, crunchTxn *txn, uint8_t indexID) {

}

/**
 *
 * @brief
 *
 * @param tableRow
 * @param indexID
 * @return index message
 */
std::unique_ptr<capnp::MallocMessageBuilder>
crunch::build_index(std::shared_ptr<capnp::MallocMessageBuilder> tableRowMessage,
                    uint8_t indexID) {
  if (indexSchemas.find(indexID) == indexSchemas.end())
    return nullptr;

  capnp::StructSchema indexSchema = indexSchemas[indexID];

  std::unique_ptr<capnp::MallocMessageBuilder> indexRow = std::make_unique<capnp::MallocMessageBuilder>();
  capnp::DynamicStruct::Builder rowBuilder = indexRow->initRoot<capnp::DynamicStruct>(indexSchema);

  capnp::DynamicStruct::Reader tableRow = capnp::FlatArrayMessageReader(
      capnp::messageToFlatArray(tableRowMessage->getSegmentsForOutput())).getRoot<capnp::DynamicStruct>(indexSchema);

  for (capnp::StructSchema::Field indexField : indexSchema.getFields()) {
    auto fieldName = indexField.getProto().getName();
    rowBuilder.set(fieldName, tableRow.get(fieldName));

  }

  return indexRow;

}

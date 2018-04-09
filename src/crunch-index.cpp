/*
** Created by Seth Shelnutt on 3/4/18.
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include <iostream>
#include <libgen.h>
#include <key.h>
#include "crunch.hpp"
#include "utils.hpp"
#include "crunchrowlocation.capnp.h"

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
int crunch::build_and_write_indexes(uchar *buf, std::shared_ptr<capnp::MallocMessageBuilder> tableRow,
                                    schema schemaForMessage, crunchTxn *txn) {

  int rc = 0;
  for (auto index : indexSchemas) {
    // Build index
    std::unique_ptr<capnp::MallocMessageBuilder> indexRow =
        build_index(buf, tableRow, schemaForMessage, index.first, txn);
    // Write index
    ::crunchy::index index1 = indexSchemas[index.first];
/*    StringPtr &indexKey = indexRow->getRoot<capnp::DynamicStruct>(index1.schema)
                .get(CRUNCH_INDEX_KEY_FIELD_NAME).as<capnp::Text>().asString();*/

        crunchy::key indexKey(indexRow->getRoot<capnp::DynamicStruct>(index1.schema).get(CRUNCH_INDEX_KEY_FIELD_NAME).as<capnp::Text>().asBytes().asBytes().begin(),
                     table->key_info[index.first]);

    for (auto &indexMap : unConsolidatedUniqueIndexes) {
      if (index1.indexFlags & HA_NOSAME && indexMap.second->find(indexKey) != indexMap.second->end()) {
        print_keydup_error(table, &table->key_info[indexMap.first], MYF(0));
        return HA_ERR_FOUND_DUPP_KEY;
      }
    }
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
  // Use a message builder for each index row
  try {

    int fd = txn->getTransactionIndexFileDescriptor(this->name, indexID);
    // Set the fileDescriptor to the end of the file
    lseek(fd, 0, SEEK_END);
    //Write message to file
    capnp::writeMessageToFd(fd, *indexRow);

  } catch (kj::Exception e) {
    std::cerr << "exception on table " << name << " for write_index, line: " << __FILE__ << ":" << __LINE__
              << ", exception_line: " << e.getFile() << ":" << e.getLine()
              << ", type: " << (int) e.getType()
              << ", e.what(): " << e.getDescription().cStr() << std::endl;
    txn->isTxFailed = true;
    return -322;
  } catch (const std::exception &e) {
    // Log errors
    std::cerr << "write index error for table " << name << ": " << e.what() << std::endl;
    txn->isTxFailed = true;
    return 322;
  }

  return 0;
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
crunch::build_index(uchar *buf, std::shared_ptr<capnp::MallocMessageBuilder> tableRowMessage,
                    schema schemaForMessage, uint8_t indexID, crunchTxn *txn) {
  if (indexSchemas.find(indexID) == indexSchemas.end())
    return nullptr;

  // Get schema of index
  capnp::StructSchema indexSchema = indexSchemas[indexID].schema;

  std::unique_ptr<capnp::MallocMessageBuilder> indexRow = std::make_unique<capnp::MallocMessageBuilder>();
  capnp::DynamicStruct::Builder rowBuilder = indexRow->initRoot<capnp::DynamicStruct>(indexSchema);


  capnp::DynamicStruct::Builder tableRow = tableRowMessage->getRoot<capnp::DynamicStruct>(schemaForMessage.schema);

  // Set crunch row location struct
  capnp::DynamicStruct::Builder builder = rowBuilder.init(
      CRUNCH_ROW_LOCATION_STRUCT_FIELD_NAME).as<capnp::DynamicStruct>();
  // Get datafile base name
  std::string DataFile = txn->getTransactionDataFile(name);
  char *buff = new char[DataFile.size() + 1];
  strcpy(buff, DataFile.c_str());

  builder.set("fileName", basename(buff));
  builder.set("rowEndLocation", 0);
  builder.set("rowStartLocation", 1);


  capnp::StructSchema::FieldList indexFields = indexSchema.getFields();
  // Skip first field as it is rowlocation struct
  for (uint i = NON_MYSQL_INDEX_FIELD_COUNT; i < indexFields.size(); i++) {
    auto fieldName = indexFields[i].getProto().getName();
    rowBuilder.set(fieldName, tableRow.get(fieldName).asReader());
  }
  uchar *to_key = new uchar[table->key_info[indexID].key_length];
  key_copy(to_key, buf, &table->key_info[indexID], table->key_info[indexID].key_length);
  kj::ArrayPtr<kj::byte> kjArray(to_key, table->key_info[indexID].key_length);
  rowBuilder.set(CRUNCH_INDEX_KEY_FIELD_NAME, capnp::Text::Builder(capnp::Data::Builder(kjArray).asChars().begin()).asReader());

  return indexRow;
}

int crunch::readIndexIntoBTree(int indexFileDescriptor, indexFile indexStruct) {
  long size = lseek(indexFileDescriptor, 0, SEEK_END); // seek to end of file
  lseek(indexFileDescriptor, 0, SEEK_SET); // seek back to beginning of file
  if (size > 0) {
    if (indexSchemas.find(indexStruct.indexID) == indexSchemas.end())
      return -1;
    crunchy::index indexSchema = indexSchemas[indexStruct.indexID];
    while (lseek(indexFileDescriptor, 0, SEEK_CUR) != size) {
      try {
        capnp::StreamFdMessageReader message(indexFileDescriptor);
        capnp::DynamicStruct::Reader indexRow = message.getRoot<capnp::DynamicStruct>(indexSchema.schema);

        if (indexSchema.indexFlags & HA_NOSAME) {
          std::unique_ptr<crunchy::crunchUniqueIndexMap>& index = unConsolidatedUniqueIndexes[indexStruct.indexID];
          if(index == nullptr) {
            index = std::make_unique<crunchy::crunchUniqueIndexMap>();
          }

          // TODO: check if inserted successfully
          crunchy::key keyToAdd(indexRow.get(CRUNCH_INDEX_KEY_FIELD_NAME).as<capnp::Text>().asBytes().asBytes().begin(),
                                            table->key_info[indexStruct.indexID]);
          auto insertedPair = index->insert(std::pair<crunchy::key, capnp::DynamicStruct::Reader>(keyToAdd, indexRow));

        } else {
          std::unique_ptr<crunchy::crunchIndexMap>& index = unConsolidatedIndexes[indexStruct.indexID];
          if(index == nullptr) {
            index = std::make_unique<crunchy::crunchIndexMap>();
          }

          // TODO: check if inserted successfully
          crunchy::key keyToAdd(indexRow.get(CRUNCH_INDEX_KEY_FIELD_NAME).as<capnp::Text>().asBytes().asBytes().begin(),
                                table->key_info[indexStruct.indexID]);
          auto insertedPair = index->insert(std::pair<crunchy::key, capnp::DynamicStruct::Reader>(keyToAdd, indexRow));
        }

      } catch (kj::Exception e) {
        if (e.getDescription() != "expected n >= minBytes; Premature EOF") {
          std::cerr << "exception: " << e.getFile() << ", line: " << __FILE__ << ":" << __LINE__
                    << ", exception_line: "
                    << e.getLine() << ", type: " << (int) e.getType()
                    << ", e.what(): " << e.getDescription().cStr() << std::endl;
          return -1;
        } else {
          break;
        }
      } catch (std::exception e) {
        std::cerr << "crunch: Error reading delete file: " << e.what() << std::endl;
        return -1;
      }
    }
  }
  return 0;
}

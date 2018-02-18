/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include <cstdint>
#include "crunch-alter-ctx.hpp"
#include "utils.hpp"
#include "capnp-mysql.hpp"

/**
 *
 * Ctx for handling in place alter tables
 *
 * @param baseDirectory
 * @param transactionDirectory
 * @param alteredTable
 * @param schemaVersion
 */
crunchInplaceAlterCtx::crunchInplaceAlterCtx(std::string baseDirectory, std::string transactionDirectory,
                                             TABLE *alteredTable, uint64_t schemaVersion) {
  this->baseDirectory = baseDirectory;
  this->transactionDirectory = transactionDirectory;
  this->alteredTable = alteredTable;
  this->schemaVersion = schemaVersion;
  this->fields = std::vector<Field*>(alteredTable->field, alteredTable->field + alteredTable->s->fields);
}

/**
 * @brief Build a new capnp schema based off alter table, but store in transaction dir.
 *
 * @return false on success, true on failure
 */
bool crunchInplaceAlterCtx::buildNewCapnpSchema() {
  char name_buff[FN_REFLEN];
  File create_file;
  int err = 0;
  // Set new schema version to current version +1;
  uint64_t newSchemaVersion = this->schemaVersion + 1;
  std::string schemaName = parseFileNameForStructName(this->baseDirectory);
  std::string newSchema = buildCapnpLimitedSchema(this->fields, schemaName, &err, 0, newSchemaVersion,
                                                  this->schemaVersion);

  //Build base schemaFileName
  this->schemaFileName =
      this->alteredTable->s->table_name.str + ("." + std::to_string(newSchemaVersion) + TABLE_SCHEME_EXTENSION);
  // Let mysql create the file for us
  this->transactionSchemaFile = fn_format(name_buff, this->schemaFileName.c_str(), this->transactionDirectory.c_str(),
                                          ("." + std::to_string(this->schemaVersion + 1) +
                                           TABLE_SCHEME_EXTENSION).c_str(),
                                          MY_UNPACK_FILENAME);
  // Create new schema file in transaction directory
  if ((create_file = my_create(this->transactionSchemaFile.c_str(), 0,
                               O_RDWR | O_TRUNC, MYF(MY_WME))) < 0)
    return true;

  // Write the capnp schema to schema file
  ::write(create_file, newSchema.c_str(), newSchema.length());
  my_close(create_file, MYF(0));
  return false;
}

/**
 * @brief Commit inplace alter table
 *
 * @return false on success, true on failure
 */
bool crunchInplaceAlterCtx::commit() {

  char name_buff[FN_REFLEN];
  if (!this->transactionSchemaFile.empty() && getFilesize(this->transactionSchemaFile.c_str()) > 0) {
    std::string renameFile = fn_format(name_buff, this->schemaFileName.c_str(), this->baseDirectory.c_str(),
                                       "",
                                       MY_UNPACK_FILENAME);
    int res = my_rename(this->transactionSchemaFile.c_str(), renameFile.c_str(), 0);
    // If rename was not successful return and rollback
    if (res)
      return true;
  }
  return false;
}

/**
 *
 * @brief Rollback inplace alter table
 *
 * @return false on success, true on failure
 */
bool crunchInplaceAlterCtx::rollback() {
  int res = 0;
  // Delete transaction file on rollback
  if (!this->transactionSchemaFile.empty() && checkFileExists(this->transactionSchemaFile.c_str())) {
    res = my_delete(this->transactionSchemaFile.c_str(), 0);
    if (res)
      return true;
  }
  return false;
}

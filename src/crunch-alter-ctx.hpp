/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#ifndef CRUNCH_CRUNCH_ALTER_CTX_HPP
#define CRUNCH_CRUNCH_ALTER_CTX_HPP

#include <handler.h>
#include <table.h>
#include <cstdint>
#include <vector>


/**
 * @breif In-place alter handler context.
 */
class crunchInplaceAlterCtx : public inplace_alter_handler_ctx {

public:
    crunchInplaceAlterCtx(std::string baseDirectory, std::string transactionDirectory, TABLE *alteredTable,
                          uint64_t schemaVersion);

    ~crunchInplaceAlterCtx() {}

    bool buildNewCapnpSchema();

    bool commit();

    bool rollback();

    std::vector<Field*> fields;

private:
    std::string baseDirectory;
    std::string transactionDirectory;
    TABLE *alteredTable;
    uint64_t schemaVersion;
    std::string schemaFileName;
    std::string transactionSchemaFile;
};

#endif // CRUNCH_CRUNCH_ALTER_CTX_HPP

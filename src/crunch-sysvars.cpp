/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include <handler.h>
#include "crunch-sysvars.hpp"

static MYSQL_THDVAR_ULONG(consolidation_threshold, PLUGIN_VAR_OPCMDARG,
                          "Threshold for consolidation of datafiles of crunch tables. Defaults to 100 files", NULL, NULL, 100, 0, ~0UL, 0);

struct st_mysql_sys_var* crunch_system_variables[]= {
    MYSQL_SYSVAR(consolidation_threshold),
    NULL
};

// Array of table options
ha_create_table_option crunch_table_options[] =
    {
        HA_TOPTION_SYSVAR("consolidation_threshold", consolidation_threshold, consolidation_threshold),
        HA_TOPTION_END
    };
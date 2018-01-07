/*
** Licensed under the GNU Lesser General Public License v3 or later
*/

#ifndef CRUNCH_SYSVARS_HPP
#define CRUNCH_SYSVARS_HPP

extern struct st_mysql_sys_var* crunch_system_variables[];

// Structure for defining table options
struct ha_table_option_struct {
    ulonglong consolidation_threshold;
};

// Array of table options
extern ha_create_table_option crunch_table_options[];
#endif
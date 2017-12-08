/*
** Created by Seth Shelnutt on 7/10/17.
** Licensed under the GNU Lesser General Public License v3 or later
*/

#ifndef CRUNCH_UTILS_HPP
#define CRUNCH_UTILS_HPP

#include <string>
#include <vector>

#define TABLE_SCHEME_EXTENSION ".capnp"
#define TABLE_DATA_EXTENSION ".capnpd"
#define TABLE_DELETE_EXTENSION ".deleted.capnpd"
#define TABLE_TRANSACTION_DIRECTORY "transactions"

/** Split string into a vector by regex
 *
 * @param input
 * @param regex
 * @return
 */
std::vector<std::string> split(const std::string& input, const std::string& regex);

/** Parse a mysql file path and get the corresponding cap'n proto struct name
 *
 * @param filepathName
 * @return
 */
std::string parseFileNameForStructName(std::string filepathName);


/**
 * Get the size of a file
 * @param filename
 * @return
 */
size_t getFilesize(const char* filename);

/**
 * Create a directory
 * @param dir
 * @return error code
 */
int createDirectory(std::string dir);

int remove_directory(const char *path);

std::vector<std::string> read_directory(const std::string& name);

int is_fd_valid(int fd);

#endif //CRUNCH_UTILS_HPP

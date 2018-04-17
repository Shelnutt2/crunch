/*
** Created by Seth Shelnutt on 7/10/17.
** Licensed under the GNU Lesser General Public License v3 or later
*/

#ifndef CRUNCH_UTILS_HPP
#define CRUNCH_UTILS_HPP

#include <string>
#include <vector>
#include <regex>
#include <kj/string.h>
#include <capnp/dynamic.h>

#define TABLE_CONSOLIDATE_DIRECTORY "consolidate"
#define TABLE_DATA_EXTENSION ".capnpd"
#define TABLE_DELETE_EXTENSION ".deleted.capnpd"
#define TABLE_SCHEMA_EXTENSION ".capnp"
#define TABLE_INDEX_SCHEMA_EXTENSION ".index.capnp"
#define TABLE_INDEX_EXTENSION ".index.capnpd"
#define TABLE_TRANSACTION_DIRECTORY "transactions"
#define TABLE_DATA_DIRECTORY "data"

extern std::regex schemaFileExtensionRegex;
extern std::regex dataFileExtensionRegex;
extern std::regex indexSchemaFileExtensionRegex;
extern std::regex indexFileExtensionRegex;

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

/** Parse a mysql file path and get the corresponding cap'n proto index struct name
 *
 * @param filepathName
 * @return
 */
std::string parseFileNameForIndexStructName(std::string filepathName);

/**
 * Get the size of a file
 * @param filename
 * @return
 */
size_t getFilesize(const char* filename);

/**
 * Get the size of a file
 * @param filename
 * @return
 */
size_t checkFileExists(const char* filename);

/**
 * Create a directory
 * @param dir
 * @return error code
 */
int createDirectory(std::string dir);

int removeDirectory(std::string path);

std::vector<std::string> readDirectory(const std::string &name);

int isFdValid(int fd);

std::string determineSymLink(std::string file);

kj::StringPtr dynamicValueToString(capnp::DynamicValue::Reader val);


int clzl(unsigned long val);
void dynamicPrintValue(capnp::DynamicValue::Reader value);
#endif //CRUNCH_UTILS_HPP

/*
** Created by Seth Shelnutt on 7/10/17.
** Licensed under the GNU Lesser General Public License v3 or later
*/

#ifndef CRUNCH_UTILS_HPP
#define CRUNCH_UTILS_HPP

#include <string>
#include <vector>
#include <regex>
#include <sys/stat.h>

/** Split string into a vector by regex
 *
 * @param input
 * @param regex
 * @return
 */
std::vector<std::string> split(const std::string& input, const std::string& regex) {
  // passing -1 as the submatch index parameter performs splitting
  std::regex re(regex);
  std::sregex_token_iterator
  first{input.begin(), input.end(), re, -1},
  last;
  return {first, last};
}

/** Parse a mysql file path and get the corresponding cap'n proto struct name
 *
 * @param filepathName
 * @return
 */
std::string parseFileNameForStructName(std::string filepathName) {
  std::vector<std::string> parts = split(filepathName, "/");
  std::string name = parts[parts.size()-1];
  name[0] = toupper(name[0]);
  return name;
}

/**
 * Get the size of a file
 * @param filename
 * @return
 */
size_t getFilesize(const char* filename) {
  struct stat st;
  stat(filename, &st);
  return st.st_size;
}


#endif //CRUNCH_UTILS_HPP

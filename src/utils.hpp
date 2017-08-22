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
#include <dirent.h>
#include <sys/types.h>

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

/**
 * Create a directory
 * @param dir
 * @return error code
 */
int createDirectory(std::string dir) {
  mode_t nMode = 0755; // UNIX style permissions
  int nError = 0;
#if defined(_WIN32)
  nError = _mkdir(dir.c_str()); // can be used on Windows
#else
  nError = mkdir(dir.c_str(), nMode); // can be used on non-Windows
#endif
  return nError;
}

/**
 * Remove a directory
 * @param dir
 * @return error code
 *//*
int rmDirectory(std::string dir) {
  int nError = 0;
#if defined(_WIN32)
  nError = _rmdir(dir.c_str()); // can be used on Windows
#else
  nError = rmdir(dir.c_str()); // can be used on non-Windows
#endif
  return nError;
}*/

int remove_directory(const char *path) {
  DIR *d = opendir(path);
  size_t path_len = strlen(path);
  int r = -1;

  if (d) {
    struct dirent *p;

    r = 0;

    while (!r && (p=readdir(d))) {
      int r2 = -1;
      char *buf;
      size_t len;

      /* Skip the names "." and ".." as we don't want to recurse on them. */
      if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
        continue;
      }

      len = path_len + strlen(p->d_name) + 2;
      buf = (char*)malloc(len);

      if (buf) {
        struct stat statbuf;

        snprintf(buf, len, "%s/%s", path, p->d_name);

        if (!stat(buf, &statbuf)) {
          if (S_ISDIR(statbuf.st_mode)) {
            r2 = remove_directory(buf);
          }
          else {
            r2 = unlink(buf);
          }
        }
        free(buf);
      }
      r = r2;
    }
    closedir(d);
  }

  if (!r) {
    r = rmdir(path);
  }
  return r;
}



#endif //CRUNCH_UTILS_HPP

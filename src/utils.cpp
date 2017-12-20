/*
** Created by Seth Shelnutt on 7/10/17.
** Licensed under the GNU Lesser General Public License v3 or later
*/

#include "utils.hpp"
#include <string>
#include <vector>
#include <regex>
#include <sys/stat.h>
#if defined(_WIN32)
#include <windows.h>
#else
#include <sys/types.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <my_sys.h>
#include <iostream>

#endif


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
 * Get the size of a file
 * @param filename
 * @return
 */
size_t checkFileExists(const char* filename) {
  struct stat st;
  return (stat(filename, &st) == 0);
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

int removeDirectory(std::string pathString) {
  const char* path = pathString.c_str();
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
            r2 = removeDirectory(buf);
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

/**
 * Read contents of a directory
 * @param name
 * @return vector of files/directories in directory
 */
std::vector<std::string> readDirectory(const std::string &name)
{
  std::vector<std::string> v;
#if defined(_WIN32)
  std::string pattern(name);
  pattern.append("\\*");
  WIN32_FIND_DATA data;
  HANDLE hFind;
  if ((hFind = FindFirstFile(pattern.c_str(), &data)) != INVALID_HANDLE_VALUE) {
      do {
          v.push_back(data.cFileName);
      } while (FindNextFile(hFind, &data) != 0);
      FindClose(hFind);
  }
#else
  DIR* dirp = opendir(name.c_str());
  struct dirent * dp;
  while ((dp = readdir(dirp)) != NULL) {
    if(strcmp(dp->d_name, ".") && strcmp(dp->d_name, ".."))
      v.push_back(dp->d_name);
  }
  closedir(dirp);
#endif
  return v;
}

int isFdValid(int fd)
{
  return fd > 0 && (fcntl(fd, F_GETFD) != -1 || errno != EBADF);
}

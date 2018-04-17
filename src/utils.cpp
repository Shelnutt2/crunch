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
#include <limits.h>

#endif

std::regex schemaFileExtensionRegex(".(\\d+)" + std::string(TABLE_SCHEMA_EXTENSION));
std::regex dataFileExtensionRegex(".(\\d+)" + std::string(TABLE_DATA_EXTENSION));
std::regex indexSchemaFileExtensionRegex(".(\\d+)" + std::string(TABLE_INDEX_SCHEMA_EXTENSION));
std::regex indexFileExtensionRegex(".(\\d+)" + std::string(TABLE_INDEX_EXTENSION));

/** Split string into a vector by regex
 *
 * @param input
 * @param regex
 * @return
 */
std::vector<std::string> split(const std::string &input, const std::string &regex) {
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
  std::string name = parts[parts.size() - 1];
  // Cap'n Proto schema do not allow special characters
  name.resize(std::remove_if(name.begin(), name.end(), [](char x) { return !isalnum(x); }) - name.begin());
  // Cap'n Proto schema's require the first character to be upper case for struct names
  name[0] = toupper(name[0]);
  return name;
}

/** Parse a mysql file path and get the corresponding cap'n proto index struct name
 *
 * @param filepathName
 * @return
 */
std::string parseFileNameForIndexStructName(std::string filepathName) {
  std::vector<std::string> parts = split(filepathName, "/");
  std::string name = parts[parts.size() - 1];
  // Split to remove extension and indexID
  parts = split(name, "\\.");
  name = parts[0];
  // Split to remove table name
  parts = split(name, "-");
  name = parts[parts.size() - 1];
  // Cap'n Proto schema do not allow special characters
  name.resize(std::remove_if(name.begin(), name.end(), [](char x) { return !isalnum(x); }) - name.begin());
  // Cap'n Proto schema's require the first character to be upper case for struct names
  name[0] = toupper(name[0]);
  return name;
}

/**
 * Get the size of a file
 * @param filename
 * @return
 */
size_t getFilesize(const char *filename) {
  struct stat st;
  stat(filename, &st);
  return st.st_size;
}

/**
 * Get the size of a file
 * @param filename
 * @return
 */
size_t checkFileExists(const char *filename) {
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
  const char *path = pathString.c_str();
  DIR *d = opendir(path);
  size_t path_len = strlen(path);
  int r = -1;

  if (d) {
    struct dirent *p;

    r = 0;

    while (!r && (p = readdir(d))) {
      int r2 = -2;
      char *buf;
      size_t len;

      /* Skip the names "." and ".." as we don't want to recurse on them. */
      if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
        continue;
      }

      len = path_len + strlen(p->d_name) + 2;
      buf = (char *) malloc(len);

      if (buf) {
        struct stat statbuf;

        snprintf(buf, len, "%s/%s", path, p->d_name);

        if (!lstat(buf, &statbuf)) {
          if (S_ISDIR(statbuf.st_mode)) {
            r2 = removeDirectory(buf);
          } else {
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
std::vector<std::string> readDirectory(const std::string &name) {
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
  DIR *dirp = opendir(name.c_str());
  struct dirent *dp;
  while ((dp = readdir(dirp)) != NULL) {
    // Skip transaction directories to avoid reading those files
    if (strcmp(dp->d_name, ".") && strcmp(dp->d_name, "..")
        && strcmp(dp->d_name, TABLE_TRANSACTION_DIRECTORY)
        && strcmp(dp->d_name, TABLE_CONSOLIDATE_DIRECTORY)) {
      // If the type is a directory we should recursively read it
      if (dp->d_type == DT_DIR) {
        std::vector<std::string> v1 = readDirectory(name + "/" + dp->d_name);
        v.insert(v.end(), v1.begin(), v1.end());
      } else if (dp->d_type == DT_REG) {
        v.push_back(name + "/" + dp->d_name);
      }
    }
  }
  closedir(dirp);
#endif
  return v;
}

int isFdValid(int fd) {
  return fd > 0 && (fcntl(fd, F_GETFD) != -1 || errno != EBADF);
}

std::string determineSymLink(std::string file) {
  char buff[PATH_MAX];
  ssize_t len = ::readlink(file.c_str(), buff, sizeof(buff) - 1);
  if (len != -1) {
    buff[len] = '\0';
    return std::string(buff);
  }
  return "";
}

kj::StringPtr dynamicValueToString(capnp::DynamicValue::Reader value) {
  // Print an arbitrary message via the dynamic API by
  // iterating over the schema.  Look at the handling
  // of STRUCT in particular.

  kj::StringPtr valString;
  switch (value.getType()) {
    case capnp::DynamicValue::VOID:
      break;
    case capnp::DynamicValue::BOOL:
      valString = (value.as<bool>() ? "1" : "0");
      break;
    case capnp::DynamicValue::INT:
      valString = std::to_string(value.as<int64_t>());
      break;
    case capnp::DynamicValue::UINT:
      valString = std::to_string(value.as<uint64_t>());
      break;
    case capnp::DynamicValue::FLOAT:
      valString = std::to_string(value.as<double>());
      break;
    case capnp::DynamicValue::TEXT:
      valString = value.as<capnp::Text>();
    default:
      break;
  }
  return valString;
}

/**
 * Count number of leading zeros
 * @param val to count leading zeros of
 * @return leading zeros
 */
int clzl(unsigned long val) {
  return __builtin_clz(val);
}

void dynamicPrintValue(capnp::DynamicValue::Reader value) {
  // Print an arbitrary message via the dynamic API by
  // iterating over the schema.  Look at the handling
  // of STRUCT in particular.

  switch (value.getType()) {
    case capnp::DynamicValue::VOID:
      std::cout << "";
      break;
    case capnp::DynamicValue::BOOL:
      std::cout << (value.as<bool>() ? "true" : "false");
      break;
    case capnp::DynamicValue::INT:
      std::cout << value.as<int64_t>();
      break;
    case capnp::DynamicValue::UINT:
      std::cout << value.as<uint64_t>();
      break;
    case capnp::DynamicValue::FLOAT:
      std::cout << value.as<double>();
      break;
    case capnp::DynamicValue::TEXT:
      std::cout << '\"' << value.as<capnp::Text>().cStr() << '\"';
      break;
    case capnp::DynamicValue::LIST: {
      std::cout << "[";
      bool first = true;
      for (auto element: value.as<capnp::DynamicList>()) {
        if (first) {
          first = false;
        } else {
          std::cout << ", ";
        }
        dynamicPrintValue(element);
      }
      std::cout << "]";
      break;
    }
    case capnp::DynamicValue::ENUM: {
      auto enumValue = value.as<capnp::DynamicEnum>();
      KJ_IF_MAYBE(enumerant, enumValue.getEnumerant()) {
        std::cout <<
                  enumerant->getProto().getName().cStr();
      } else {
        // Unknown enum value; output raw number.
        std::cout << enumValue.getRaw();
      }
      break;
    }
    case capnp::DynamicValue::STRUCT: {
      std::cout << "(";
      auto structValue = value.as<capnp::DynamicStruct>();
      bool first = true;
      for (auto field: structValue.getSchema().getFields()) {
        if (!structValue.has(field)) continue;
        if (first) {
          first = false;
        } else {
          std::cout << ", ";
        }
        std::cout << field.getProto().getName().cStr()
                  << " = ";
        dynamicPrintValue(structValue.get(field));
      }
      std::cout << ")";
      break;
    }
    default:
      // There are other types, we aren't handling them.
      std::cout << "?";
      break;
  }
}
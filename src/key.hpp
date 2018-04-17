//
// Created by Seth Shelnutt on 4/8/18.
//

#ifndef MYSQL_KEY_HPP
#define MYSQL_KEY_HPP

#pragma once

#include <key.h>
#include "utils.hpp"

namespace crunchy {
  class key {

  public:
      key() = default;

      key(const uchar *key_data, KEY key_info) {
        this->key_data = std::unique_ptr<uchar[]>(new uchar[key_info.key_length]);
        std::memcpy(this->key_data.get(), (void *) key_data, key_info.key_length);
        this->key_info = key_info;
      }

      key(const uchar *key_data, KEY key_info, key_part_map keypart_map) : key(key_data, key_info){
        // Currently the key_part_map only supports prefixes (mariadb/mysql limitation)
        // thus we can find the most significant bit to determine the number of keys present
        uint parts = 64 - clzl(keypart_map);
        // Next we modify the key info for this;
        key_info.user_defined_key_parts=parts;
      }

      // Copy constructor
      key(const key &k2) {
        this->key_info = k2.key_info;
        this->key_data = std::unique_ptr<uchar[]>(new uchar[k2.key_info.key_length]);
        std::memcpy(this->key_data.get(), k2.key_data.get(), k2.key_info.key_length);
      }

      // Move constructor
      key(key&& other) noexcept
      {
        this->key_data = std::move(other.key_data);
        other.key_data = nullptr;
        this->key_info = other.key_info;
      }

      ~key() {};

      /**
       *
       * @param key0
       * @param key1
       * @return 0 is keys are equal, -1 if key0 < key1 and 1 if key0 > key1
       */
      static int CmpKeys(const uchar *key0, const uchar *key1, const KEY *key_info) {
        int res = 0;
        for (size_t i = 0; i < key_info->user_defined_key_parts && !res; i++) {
          const auto &keyPart = key_info->key_part[i];
          const int off = (keyPart.null_bit ? 1 : 0); // to step over null-byte

          if (keyPart.null_bit) // does the key part have a null-byte?
            if (*key0 != *key1) {
              return (int) ((*key1) - (*key0));
            }
          res = keyPart.field->key_cmp(key0 + off, key1 + off); // compare key parts
          key0 += keyPart.store_length; // go to next key part
          key1 += keyPart.store_length;
        }
        return res;
      }

      bool operator==(const key &b) const {
        return CmpKeys(this->key_data.get(), b.key_data.get(), &this->key_info) == 0;
      };

      bool operator!=(const key &b) const {
        return CmpKeys(this->key_data.get(), b.key_data.get(), &this->key_info) != 0;
      };

      bool operator<(const key &b) const {
        return CmpKeys(this->key_data.get(), b.key_data.get(), &this->key_info) == -1;
      };

      bool operator>(const key &b) const {
        return CmpKeys(this->key_data.get(), b.key_data.get(), &this->key_info) == 1;
      };

      bool operator<=(const key &b) const {
        return CmpKeys(this->key_data.get(), b.key_data.get(), &this->key_info) <= 0;
      };

      bool operator>=(const key &b) const {
        return CmpKeys(this->key_data.get(), b.key_data.get(), &this->key_info) >= 0;
      };

      // assignment operator
      key& operator=(const key& rhs) {
        if (&rhs != this) {
          key tmp(rhs);
          std::swap(*this, tmp);
        }
        return *this;
      }

      // move assignment operator
      key& operator=(key&& rhs) noexcept {
        std::swap(this->key_data, rhs.key_data);
        std::swap(this->key_info, rhs.key_info);
        return *this;
      }

  private:
      friend std::ostream &operator<<(std::ostream &strm, const key &k) {
        for (uint i = 0; i < k.key_info.key_length; i++) {
          strm << static_cast<unsigned>(k.key_data[i]);
        }
        return strm;
      }

      std::unique_ptr<uchar[]> key_data;
      KEY key_info;
  };

  typedef btree::btree_map<crunchy::key, capnp::DynamicStruct::Reader> crunchUniqueIndexMap;
  typedef btree::btree_multimap<crunchy::key, capnp::DynamicStruct::Reader> crunchIndexMap;
  //typedef std::map<crunchy::key, capnp::DynamicStruct::Reader> crunchUniqueIndexMap;
  //typedef std::multimap<crunchy::key, capnp::DynamicStruct::Reader> crunchIndexMap;
}


#endif //MYSQL_KEY_HPP

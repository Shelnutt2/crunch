//
// Created by Seth Shelnutt on 4/8/18.
//

#ifndef MYSQL_KEY_HPP
#define MYSQL_KEY_HPP

#pragma once

#include <key.h>

namespace crunchy {
  class key {

  public:
      key() = default;

      key(const uchar *key_data, KEY key_info) {
        this->key_data = std::unique_ptr<uchar[]>(new uchar[key_info.key_length]);
        std::memcpy(this->key_data.get(), (void *) key_data, key_info.key_length);
        this->key_info = key_info;
      }

      // Copy constructor
      key(const key &k2) {
        this->key_info = k2.key_info;
        this->key_data = std::unique_ptr<uchar[]>(new uchar[k2.key_info.key_length]);
        std::memcpy(this->key_data.get(), k2.key_data.get(), k2.key_info.key_length);
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

  typedef std::map<crunchy::key, capnp::DynamicStruct::Reader> crunchUniqueIndexMap;
  typedef std::multimap<crunchy::key, capnp::DynamicStruct::Reader> crunchIndexMap;
}


#endif //MYSQL_KEY_HPP

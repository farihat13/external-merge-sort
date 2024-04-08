#ifndef RECORD_H
#define RECORD_H

#include "config.h"
#include <cstring>


/**
 * @brief Record class
 */
class Record {
  public:
    Record(char *_data) : _data(_data){};
    ~Record() { delete[] _data; }
    bool operator<(const Record &other) const {
        return std::strncmp(_data, other._data, Config::RECORD_KEY_SIZE) < 0;
    }
    char *_data;
}; // class Record

#endif // RECORD_H
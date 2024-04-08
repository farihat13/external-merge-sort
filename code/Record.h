#ifndef RECORD_H
#define RECORD_H


/**
 * @brief Record class
 */
class Record {
  public:
    Record(char *_data) : _data(_data){};
    ~Record() { delete[] _data; }

  private:
    char *_data;
}; // class Record

#endif // RECORD_H
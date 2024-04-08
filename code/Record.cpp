#include "Record.h"

Record::Record(char *_data) : _data(_data){};
Record::~Record() { delete[] _data; }

#include "Record.h"


// =========================================================
// ------------------------- Record -------------------------
// =========================================================


// to string
char *Record::reprKey() {
    char *key = new char[Config::RECORD_KEY_SIZE + 1];
    std::strncpy(key, data, Config::RECORD_KEY_SIZE);
    key[Config::RECORD_KEY_SIZE] = '\0';
    return key;
}

char *Record::repr() {
    char *key = new char[Config::RECORD_SIZE + 1];
    std::strncpy(key, data, Config::RECORD_SIZE);
    key[Config::RECORD_SIZE] = '\0';
    return key;
}

bool Record::isValid() {
    if (data == nullptr) return false;
    if (this == getMaxRecord()) return true;
    for (int i = 0; i < Config::RECORD_SIZE; i++) {
        if (!isalnum(data[i])) return false;
    }
    return true;
}

Record *MAX_RECORD = nullptr;
Record *getMaxRecord() {
    if (MAX_RECORD == nullptr) {
        MAX_RECORD = new Record();
        for (int i = 0; i < Config::RECORD_SIZE; i++) {
            MAX_RECORD->data[i] = '~';
        }
    }
    return MAX_RECORD;
}
bool isRecordMax(Record *r) {
    char a = getMaxRecord()->data[0];
    char b = r->data[0];
    return a == b;
}

// =========================================================
// -------------------------- Page -------------------------
// =========================================================


// =========================================================
// ------------------------- RunReader ---------------------
// =========================================================


// =========================================================
// ----------------------- RunWriter -----------------------
// =========================================================


RowCount RunWriter::writeFromFile(std::string writeFromFilename, RowCount toCopyNRecords) {
    std::ifstream is(writeFromFilename, std::ios::binary);
    if (!is) { throw std::runtime_error("Error: Opening file " + writeFromFilename); }
    ByteCount bufSize = RoundUp(1024 * 1024, Config::RECORD_SIZE);
    char *buffer = new char[bufSize];
    ByteCount total = 0;
    while (is) {
        is.read(buffer, bufSize);
        ByteCount n = is.gcount();
        _os.write(buffer, n);
        total += n;
    }
    delete[] buffer;
    if (!_os) { throw std::runtime_error("Error: Writing to file"); }
    RowCount nRecords = total / Config::RECORD_SIZE;
    currSize += nRecords;
    printv("\t\t\t\tRunWriter copied %llu out of %llu records from %s to %s\n", nRecords,
           toCopyNRecords, writeFromFilename.c_str(), _filename.c_str());
    assert(toCopyNRecords == nRecords);
    return nRecords;
}

RowCount RunWriter::writeNextRun(Run *run) {
    char *data = run->getAllData();
    RowCount nRecords = run->getSize();
    _os.write(data, nRecords * Config::RECORD_SIZE);
    if (!_os) { throw std::runtime_error("Error: Writing to file"); }
    currSize += nRecords;
    delete[] data;
    return nRecords;
}
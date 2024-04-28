#ifndef _RECORD_H_
#define _RECORD_H_


#include "config.h"
#include "defs.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>


// =========================================================
// ------------------------- Record -------------------------
// =========================================================


class Record {
  public:
    char *data;
    Record *next = nullptr;

    /**
     * @brief Construct a new Record object,
     * This constructor will allocate memory for data
     */
    Record() {
        data = new char[Config::RECORD_SIZE];
        next = nullptr;
    }
    Record(char *data) {
        this->data = new char[Config::RECORD_SIZE];
        std::memcpy(this->data, data, Config::RECORD_SIZE);
        next = nullptr;
    }
    ~Record() {
        if (data != nullptr) {
            delete[] data;
            data = nullptr;
        }
    }

    // default comparison based on first 8 bytes of data
    bool operator<(const Record &other) const {
        return std::strncmp(data, other.data, Config::RECORD_KEY_SIZE) < 0;
    }
    bool operator>(const Record &other) const {
        return std::strncmp(data, other.data, Config::RECORD_KEY_SIZE) > 0;
    }
    // equality comparison based on all bytes of data
    bool operator==(const Record &other) const {
        return std::strncmp(data, other.data, Config::RECORD_SIZE) == 0;
    }

    // to string
    char *reprKey();
    char *repr();

    // utility functions
    bool isValid();
    // void invalidate();
}; // class Record


extern Record *MAX_RECORD;
Record *getMaxRecord();
bool isRecordMax(Record *r);


// =========================================================
// ------------------------- Run ---------------------------
// =========================================================


class Run {
  private:
    Record *runHead;
    RowCount size;

  public:
    Run(Record *head, RowCount size) : runHead(head), size(size) {}
    ~Run() {
        printv("\t\t\t\tRun destroying\n");
        flushv();
        Record *curr = runHead;
        RowCount i = 0;
        while (curr != nullptr && i < size) {
            Record *next = curr->next;
            delete curr;
            i++;
            curr = next;
        }
        printv("\t\t\t\tRun destroyed, Deleted %lld out of %lld records\n", i, size);
        flushv();
    }

    // getters
    Record *getHead() { return runHead; }
    void setHead(Record *head) { runHead = head; }
    RowCount getSize() { return size; }

    char *getAllData() {
        char *buffer = new char[size * Config::RECORD_SIZE];
        Record *curr = runHead;
        for (RowCount i = 0; i < size; i++) {
            if (curr == nullptr) {
                printv("ERROR: Run size %lld is less than expected %lld\n", i, size);
                flushv();
                throw std::runtime_error("Error: Run size is less than expected");
            }
            std::memcpy(buffer + i * Config::RECORD_SIZE, curr->data, Config::RECORD_SIZE);
            curr = curr->next;
        }
        return buffer;
    }

    bool isSorted() {
        Record *curr = runHead;
        RowCount i = 0;
        while (curr != nullptr && curr->next != nullptr) {
            if (*curr > *(curr->next)) {
                printv("ERROR: Run is not sorted %s > %s\n", curr->reprKey(),
                       curr->next->reprKey());
                flushv();
                return false;
            }
            curr = curr->next;
            i++;
        }
        i++;
        if (i != size) {
            printv("ERROR: Run size %lld is less than expected %lld\n", i, size);
            return false;
        }
        return true;
    }

    // print
    void printRun() {
        Record *curr = runHead;
        while (curr != nullptr) {
            std::cout << curr->reprKey() << std::endl;
            curr = curr->next;
        }
    }
};


// =========================================================
// ------------------------- Page --------------------------
// =========================================================

class Page {
  private:
    RowCount capacity; // max number of records
    std::vector<Record *> records;
    Page *next;

  public:
    /**
     * @brief Construct a new Page object, with a given capacity
     * The page is an vector of records pointers
     * This constructor will NOT allocate memory for records
     */
    Page(RowCount capacityInRecords) : capacity(capacityInRecords) {
        if (capacity < 0) { throw std::runtime_error("Error: Page capacity cannot be negative"); }
        this->records.reserve(capacity);
        this->next = nullptr;
    }
    ~Page() {
        for (auto rec : records) {
            delete rec;
        }
        delete &records;
    }
    // getters
    RowCount getCapacityInRecords() { return capacity; }
    RowCount getSizeInRecords() { return records.size(); }
    Record *getFirstRecord() { return records.front(); }
    Record *getLastRecord() { return records.back(); }
}; // class Page


// =========================================================
// ----------------------- RunReader -----------------------
// =========================================================

class RunReader {
  private:
    std::string filename;
    RowCount filesize;
    RowCount PAGE_SIZE_IN_RECORDS;

    std::ifstream _is;
    RowCount _nRecordsRead = 0;
    bool _isDeleted = false;

  public:
    // member functions
    RunReader(const std::string &filename, RowCount filesize, RowCount pageSizeInRecords)
        : filename(filename), filesize(filesize), PAGE_SIZE_IN_RECORDS(pageSizeInRecords),
          _is(filename, std::ios::binary) {
        if (!_is) { throw std::runtime_error("Cannot open file: " + filename); }
        _is.seekg(0, std::ios::beg);
        printv("\t\t\t\tRunReader opened '%s'\n", filename.c_str());
    }
    ~RunReader() {
        if (!_isDeleted) {
            if (_is.is_open()) { _is.close(); }
        }
        printv("\t\t\t\tRunReader destroyed '%s'\n", filename.c_str());
    }
    void close() {
        if (!_isDeleted) {
            if (_is.is_open()) { _is.close(); }
        }
        printv("\t\t\t\tRunReader closed '%s'\n", filename.c_str());
    }
    void deleteFile() {
        if (!_isDeleted) {
            if (_is.is_open()) { _is.close(); }
            if (std::remove(filename.c_str()) != 0) {
                throw std::runtime_error("Error deleting file: " + filename);
            }
            _isDeleted = true;
        }
        printv("\t\t\t\tDEBUG: RunReader Deleted '%s'\n", filename.c_str());
    }
    bool isDeletedFile() { return _isDeleted; }
    // Page *readNextPage();
    Record *readNextRecords(RowCount *nRecords) {
        TRACE(true);
        RowCount nRecordsToRead = *nRecords;
        RowCount nRecordsReadSoFar = 0;
        Record *head = new Record();
        Record *curr = head;
        ByteCount nBytesToRead = this->PAGE_SIZE_IN_RECORDS * Config::RECORD_SIZE;
        char *recData = new char[nBytesToRead];
        while (nRecordsReadSoFar < nRecordsToRead) {
            /* if EOF reached, break */
            if (_is.eof()) { break; }
            /** read one page at a time */
            _is.read(recData, nBytesToRead);
            ByteCount nBytesRead = _is.gcount();
            /* if no bytes read, delete record and break */
            if (nBytesRead == 0) { break; }
            /* if bytes read is not equal to record size, throw error */
            if (nBytesRead % Config::RECORD_SIZE != 0) {
                delete[] recData;
                std::string errorMsg = "ERROR: Read " + std::to_string(nBytesRead) +
                                       " bytes, which is not aligned with record size " +
                                       std::to_string(Config::RECORD_SIZE);
                printv("%s\n", errorMsg.c_str());
                throw std::runtime_error(errorMsg);
            }
            /** add records to linked list */
            for (RowCount i = 0; i < nBytesRead / Config::RECORD_SIZE; i++) {
                Record *rec = new Record(recData + i * Config::RECORD_SIZE);
                curr->next = rec;
                curr = rec;
                nRecordsReadSoFar++;
            }
            // printv("\t\t\t\tRead %lld records\n", nRecordsReadSoFar);
            // flushv();
        }
        /** return the head of the linked list */
        curr->next = nullptr;          // mark the end of the linked list
        *nRecords = nRecordsReadSoFar; // update the number of records read
        curr = head->next;             // skip the dummy head
        // free memory
        delete head;      // delete the dummy head
        delete[] recData; // delete the buffer
        return curr;      // return the head of the linked list
    }


    // std::vector<Page *> readNextPages(int nPages);

    // getters
    std::string getFilename() { return filename; }
    RowCount getFilesize() { return filesize; }
    RowCount getPageSizeInRecords() { return PAGE_SIZE_IN_RECORDS; }
    RowCount getNRecordsRead() { return _nRecordsRead; }
}; // class RunReader


// =========================================================
// ----------------------- RunWriter -----------------------
// =========================================================


class RunWriter {
  private:
    std::string _filename;
    std::ofstream _os;
    // ---- internal state ----
    RowCount currSize = 0;
    bool _isDeleted = false;

  public:
    RunWriter(const std::string &filename)
        : _filename(filename), _os(filename, std::ios::binary | std::ios::trunc) {
        if (!_os) { throw std::runtime_error("Cannot open file: " + filename); }
        printv("\t\t\t\tRunWriter opened '%s'\n", filename.c_str());
    }

    ~RunWriter() {
        if (_os.is_open()) { _os.close(); }
        printv("\t\t\t\tRunWriter destroyed '%s'\n", _filename.c_str());
    }

    RowCount writeNextRun(Run *run);
    RowCount writeFromFile(std::string filename, RowCount toCopyNRecords);
    void reset() {
        if (_os.is_open()) { _os.close(); }
        // truncate the file to 0 bytes
        _os.open(_filename, std::ios::binary | std::ios::trunc);
        if (!_os) { throw std::runtime_error("Cannot open file: " + _filename); }
        currSize = 0;
        printv("\t\t\t\tRunWriter RESET '%s'\n", _filename.c_str());
    }
    void close() {
        if (!_isDeleted) {
            if (_os.is_open()) { _os.close(); }
        }
        printv("\t\t\t\tRunWriter closed '%s'\n", _filename.c_str());
    }
    void deleteFile() {
        if (!_isDeleted) {
            if (_os.is_open()) { _os.close(); }
            if (std::remove(_filename.c_str()) != 0) {
                throw std::runtime_error("Error deleting file: " + _filename);
            }
            _isDeleted = true;
        }
        printv("\t\t\t\tRunReader Deleted '%s'\n", _filename.c_str());
    }
    bool isDeletedFile() { return _isDeleted; }
    // ---- getters ----
    std::string getFilename() { return _filename; }
    RowCount getCurrSize() { return currSize; }

}; // class RunWriter


#endif // _RECORD_H_

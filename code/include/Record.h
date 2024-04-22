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
    Record(char *data) : data(data), next(nullptr) {}
    ~Record() { delete[] data; }

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

    // getters
    Record *getHead() { return runHead; }
    RowCount getSize() { return size; }

    char *getAllData() {
        char *buffer = new char[size * Config::RECORD_SIZE];
        Record *curr = runHead;
        for (int i = 0; i < size; i++) {
            std::memcpy(buffer + i * Config::RECORD_SIZE, curr->data, Config::RECORD_SIZE);
            curr = curr->next;
        }
        return buffer;
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
        if (capacity < 0) {
            throw std::runtime_error("Error: Page capacity cannot be negative");
        }
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
    Page *getNext() { return next; }

    // utility functions
    /**
     * @brief Add a record to the page, and create chain of records
     */
    int addRecord(Record *rec);
    int addNextPage(Page *page) {
        // this->getLastRecord()->next = page->getFirstRecord();
        this->next = page;
        return 0;
    }

    // read/write
    /**
     * @brief
     * @return num of records read
     * @throws runtime_error if read fails
     */
    int read(std::ifstream &is);
    /**
     * @brief
     * @return num of records written
     * @throws runtime_error if write fails
     */
    int write(std::ofstream &os);

    // validation
    bool isSorted() const;
    bool isValid() const;
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

  public:
    // member functions
    RunReader(const std::string &filename, RowCount filesize, RowCount pageSizeInRecords)
        : filename(filename), filesize(filesize), PAGE_SIZE_IN_RECORDS(pageSizeInRecords),
          _is(filename, std::ios::binary) {
        if (!_is) {
            throw std::runtime_error("Cannot open file: " + filename);
        }
    }
    ~RunReader() {
        if (_is.is_open()) {
            _is.close();
        }
    }
    Page *readNextPage();
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
    std::string filename;
    std::ofstream os;
    // ---- internal state ----
    RowCount currSize = 0;

  public:
    RunWriter(const std::string &filename)
        : filename(filename), os(filename, std::ios::binary | std::ios::trunc) {
        if (!os) {
            throw std::runtime_error("Cannot open file: " + filename);
        }
        printv("\t\t\tDEBUG: RunWriter opened '%s'\n", filename.c_str());
    }

    ~RunWriter() {
        if (os.is_open()) {
            os.close();
        }
        printv("\t\t\tDEBUG: RunWriter destroyed '%s'\n", filename.c_str());
    }

    RowCount writeNextPage(Page *page);
    // RowCount writeNextPages(std::vector<Page *> &pages);
    RowCount writeNextRun(Run &run);

    // ---- getters ----
    std::string getFilename() { return filename; }
    RowCount getCurrSize() { return currSize; }
    void close() {
        if (os.is_open()) {
            os.close();
        }
        printv("\t\t\tDEBUG: RunWriter closed '%s'\n", filename.c_str());
    }
}; // class RunWriter


#endif // _RECORD_H_

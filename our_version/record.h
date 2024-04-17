#ifndef _RECORD_H_
#define _RECORD_H_


#include "common.h"
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
    Record *next;

    /**
     * @brief Construct a new Record object,
     * This constructor will allocate memory for data
     */
    Record() {
        data = new char[Config::RECORD_SIZE];
        next = nullptr;
    }
    Record(char *data) : data(data) { next = nullptr; }
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


// =========================================================
// ------------------------- Page --------------------------
// =========================================================

class Page {
  private:
    int capacity;
    std::vector<Record *> records;

  public:
    /**
     * @brief Construct a new Page object, with a given capacity
     * The page is an vector of records pointers
     * This constructor will NOT allocate memory for records
     */
    Page(int capacityInRecords) : capacity(capacityInRecords) {
        if (capacity < 0) {
            throw std::runtime_error("Error: Page capacity cannot be negative");
        }
    }
    ~Page() {
        for (auto rec : records) {
            delete rec;
        }
        delete &records;
    }

    // getters
    int capacityInRecords() { return capacity; }
    int sizeInRecords() { return records.size(); }
    Record *getFirstRecord() { return records.front(); }
    Record *getLastRecord() { return records.back(); }

    // utility functions
    /**
     * @brief Add a record to the page, and create chain of records
     */
    int addRecord(Record *rec);

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
    bool isSorted() const { return std::is_sorted(records.begin(), records.end()); }
    bool isValid() const;

    friend class RunWriter;
}; // class Page


// =========================================================
// ----------------------- RunReader -----------------------
// =========================================================

class RunReader {
  private:
    std::ifstream is;
    int PAGE_SIZE_IN_RECORDS;

  public:
    RunReader(const std::string &filename, int pageSizeInRecords)
        : is(filename, std::ios::binary), PAGE_SIZE_IN_RECORDS(pageSizeInRecords) {
        if (!is) {
            throw std::runtime_error("Cannot open file: " + filename);
        }
    }

    ~RunReader() {
        if (is.is_open()) {
            is.close();
        }
    }

    Page *readNextPage();
    std::vector<Page *> readNextPages(int nPages);
}; // class RunReader


// =========================================================
// ----------------------- RunWriter -----------------------
// =========================================================


class RunWriter {
  private:
    std::ofstream os;

  public:
    RunWriter(const std::string &filename) : os(filename, std::ios::binary | std::ios::trunc) {
        if (!os) {
            throw std::runtime_error("Cannot open file: " + filename);
        }
    }

    ~RunWriter() {
        if (os.is_open()) {
            os.close();
        }
    }

    void writeNextPage(Page *page);
    void writeNextPages(std::vector<Page *> &pages);
}; // class RunWriter


#endif // _RECORD_H_
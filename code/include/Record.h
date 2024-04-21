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
    RowCount capacityInRecords() { return capacity; }
    RowCount sizeInRecords() { return records.size(); }
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
        // printvv("DEBUG: RunWriter opened '%s'\n", filename.c_str());
    }

    ~RunWriter() {
        if (os.is_open()) {
            os.close();
        }
    }

    RowCount writeNextPage(Page *page);
    RowCount writeNextPages(std::vector<Page *> &pages);

    RowCount writeNextRun(Run &run);
}; // class RunWriter


// =========================================================
// ----------------------- RunStreamer ---------------------
// =========================================================


class RunStreamer {
  private:
    Record *currentRecord;

    // read from file
    RunReader *reader;
    Page *currentPage;
    int readAhead;

  public:
    RunStreamer(Run *run) : currentRecord(run->getHead()), reader(nullptr) {
        if (currentRecord == nullptr) {
            throw std::runtime_error("Error: RunStreamer initialized with empty run");
        }
    }

    RunStreamer(RunReader *reader, int readAhead = 1) : reader(reader), readAhead(readAhead) {
        currentPage = reader->readNextPage();
        currentRecord = currentPage->getFirstRecord();

        if (currentRecord == nullptr) {
            throw std::runtime_error("Error: RunStreamer initialized with empty run");
        }
        if (readAhead > 1) {
            readAheadPages(readAhead - 1);
        }
    }

    void readAheadPages(int nPages) {
        // printvv("DEBUG: readAheadPages(%d)\n", nPages);
        Page *page = currentPage;
        for (int i = 0; i < nPages; i++) {
            Page *p = reader->readNextPage();
            if (p == nullptr) {
                break;
            }
            page->addNextPage(p);
            page = p;
        }
    }
    // bool hasNext() { return currentRecord->next != nullptr; }

    Record *moveNext() {
        // if reader does not exist
        if (reader == nullptr) {
            if (currentRecord->next == nullptr) {
                return nullptr;
            }
            currentRecord = currentRecord->next;
            return currentRecord;
        }

        // if reader exists
        if (currentRecord == currentPage->getLastRecord()) {
            currentPage = currentPage->getNext();
            if (currentPage == nullptr) { // no more page in memory
                // read new page and set current record to first record
                currentPage = reader->readNextPage();
                if (currentPage == nullptr) {
                    return nullptr;
                }
                currentRecord = currentPage->getFirstRecord();
                if (readAhead > 1) {
                    readAheadPages(readAhead - 1);
                }
            } else { // more pages in memory
                currentRecord = currentPage->getFirstRecord();
            }
        } else {
            currentRecord = currentRecord->next;
        }
        return currentRecord;
    }

    Record *getCurrRecord() { return currentRecord; }

    // default comparison based on first 8 bytes of data
    bool operator<(const RunStreamer &other) const { return *currentRecord < *other.currentRecord; }
    bool operator>(const RunStreamer &other) const { return *currentRecord > *other.currentRecord; }
    // equality comparison based on all bytes of data
    bool operator==(const RunStreamer &other) const {
        return *currentRecord == *other.currentRecord;
    }

    char *repr() { return currentRecord->reprKey(); }
}; // class RunStreamer

#endif // _RECORD_H_
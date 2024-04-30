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
  private:
    Record(bool allocMemory) {
        data = nullptr;
        next = nullptr;
        if (allocMemory) {
            data = new char[Config::RECORD_SIZE];
            data[0] = '!';  // mark the record as invalid
            data[1] = '\0'; // null terminate the string
        }
    }

  public:
    char *data;
    Record *next = nullptr;

    /**
     * @brief Construct a new Record object,
     * This constructor will allocate memory for data
     */
    Record() {
        data = new char[Config::RECORD_SIZE];
        data[0] = '!';  // mark the record as invalid
        data[1] = '\0'; // null terminate the string
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
            next = nullptr;
        }
    }
    /**
     * @brief Construct a new Record object, without allocating memory
     * Used for wrapping existing data, without copying
     * Used in verifying the output file
     */
    static Record *wrapAsRecord(char *data) {
        Record *rec = new Record(false);
        rec->data = data;
        return rec;
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
    /**
     * @brief Construct a new Run object
     * @param head Head of the linked list of records
     * @param size Number of records in the run
     */
    Run(Record *head, RowCount size) : runHead(head), size(size) {}

    /**
     * @brief Destroy the Run object
     * @note It frees the memory allocated for each record in the run
     */
    ~Run() {
        Record *curr = runHead;
        RowCount i = 0;
        while (curr != nullptr && i < size) {
            Record *next = curr->next;
            delete curr;
            i++;
            curr = next;
        }
        flushvv();
    }

    // Getters
    Record *getHead() { return runHead; }
    void setHead(Record *head) { runHead = head; }
    RowCount getSize() { return size; }

    /**
     * @brief Get all the data from the run as a single buffer
     * @return char* buffer containing all the data
     * @note The caller is responsible for freeing the memory
     */
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

    /**
     * @brief Check if the run is sorted
     */
    bool isSorted() {
        Record *curr = runHead, *prev = nullptr;
        RowCount i = 0;
        for (; i < this->size; i++) {
            if (curr == nullptr) {
                printv("ERROR: Run size %lld, expected %lld\n", i, size);
                flushv();
                return false;
            }
            if (prev != nullptr && *prev > *curr) {
                printv("ERROR: Run is not sorted %s > %s\n", prev->reprKey(), curr->reprKey());
                flushv();
                return false;
            }
            prev = curr;
            curr = curr->next;
        }
        return true;
    }


    /**
     * @brief Print the run to stdout
     */
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
    /**
     * @brief Construct a new RunReader object
     */
    RunReader(const std::string &filename, RowCount filesize, RowCount pageSizeInRecords)
        : filename(filename), filesize(filesize), PAGE_SIZE_IN_RECORDS(pageSizeInRecords),
          _is(filename, std::ios::binary) {
        if (!_is) { throw std::runtime_error("Cannot open file: " + filename); }
        _is.seekg(0, std::ios::beg);
        printv("\t\t\t\tRunReader opened '%s'\n", filename.c_str());
    }

    /**
     * @brief Destroy the RunReader object
     * @note It closes the file if it is open
     */
    ~RunReader() {
        if (!_isDeleted) {
            if (_is.is_open()) { _is.close(); }
        }
        printv("\t\t\t\tRunReader destroyed '%s'\n", filename.c_str());
    }

    /**
     * @brief Close the reader
     */
    void close() {
        if (!_isDeleted) {
            if (_is.is_open()) { _is.close(); }
        }
        printv("\t\t\t\tRunReader closed '%s'\n", filename.c_str());
    }

    /**
     * @brief Delete the file associated with this reader
     * @note Calling this function second time will have no effect
     */
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

    /**
     * @brief Check if the file associated with this reader is deleted
     * @return true if the file is deleted
     * @note Used by RunStreamer
     */
    bool isDeletedFile() { return _isDeleted; }

    /**
     * @brief Read the next n records from the reader's file
     * @param nRecords Number of records to read, updated with actual number of records read
     * @return the head of the linked list of records
     */
    Record *readNextRecords(RowCount *nRecords);

    // Getters
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
    /**
     * @brief Construct a new RunWriter object
     */
    RunWriter(const std::string &filename)
        : _filename(filename), _os(filename, std::ios::binary | std::ios::trunc) {
        if (!_os) { throw std::runtime_error("Cannot open file: " + filename); }
        printv("\t\t\t\tRunWriter opened '%s'\n", filename.c_str());
    }

    /**
     * @brief Destroy the RunWriter object
     */
    ~RunWriter() {
        if (_os.is_open()) { _os.close(); }
        printv("\t\t\t\tRunWriter destroyed '%s'\n", _filename.c_str());
    }

    /**
     * Write the given run to this writer's file
     * @param run
     * @return number of records written
     */
    RowCount writeNextRun(Run *run);

    /**
     * Write the records from the given file name to this writer's file
     * @param writeFromFilename
     * @param toCopyNRecords
     * @return number of records copied
     */
    RowCount writeFromFile(std::string filename, RowCount toCopyNRecords);


    /**
     * @brief Reset the writer, truncate the file to 0 bytes
     */
    void reset() {
        if (_os.is_open()) { _os.close(); }
        // truncate the file to 0 bytes
        _os.open(_filename, std::ios::binary | std::ios::trunc);
        if (!_os) { throw std::runtime_error("Cannot open file: " + _filename); }
        currSize = 0;
        printv("\t\t\t\tRunWriter RESET '%s'\n", _filename.c_str());
    }

    /**
     * @brief Close the writer
     */
    void close() {
        if (!_isDeleted) {
            if (_os.is_open()) { _os.close(); }
        }
        printv("\t\t\t\tRunWriter closed '%s'\n", _filename.c_str());
    }

    /**
     * @brief Delete the file associated with this writer
     * @note Calling this function second time will have no effect
     */
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

    /**
     * @brief Check if the file associated with this writer is deleted
     * @return true if the file is deleted
     * @note Used by spill writer at the end of a session
     */
    bool isDeletedFile() { return _isDeleted; }

    // ---- getters ----
    std::string getFilename() { return _filename; }
    RowCount getCurrSize() { return currSize; }

}; // class RunWriter


#endif // _RECORD_H_

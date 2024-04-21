#ifndef _STORAGE_H_
#define _STORAGE_H_


#include "Losertree.h"
#include "Record.h"
#include "config.h"
#include "defs.h"
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <iostream>
#include <queue>
#include <sys/stat.h>
#include <vector>


// =========================================================
//  -------------------------- Storage ---------------------
// =========================================================

class Storage {
    std::string name;
    // ---- configurations ----
    ByteCount CAPACITY_IN_BYTES = 0; // in bytes
    int BANDWIDTH = 0;               // in MB/s
    double LATENCY = 0;              // in ms

    // ---- calibrable configurations ----
    RowCount PAGE_SIZE_IN_RECORDS = 0; // in records
    PageCount CLUSTER_SIZE = 0;        // in pages
    int MERGE_FAN_IN = 30;             // number of runs to merge at a time
    int MERGE_FAN_OUT = 2;

    // ---- read/write buffer ----
    std::string readFilePath, writeFilePath;
    std::ifstream readFile;
    std::ofstream writeFile;

  protected:
    // ---- internal state ----
    RowCount _mergeFanInRec;
    RowCount _mergeFanOutRec;

  public:
    Storage(std::string name, ByteCount capacity, int bandwidth, double latency);

    // setup
    void configure();

    // getters
    ByteCount getCapacityInBytes() const { return CAPACITY_IN_BYTES; }
    RowCount getCapacityInRecords() const { return CAPACITY_IN_BYTES / Config::RECORD_SIZE; }
    ByteCount getPageSizeInBytes() const { return PAGE_SIZE_IN_RECORDS * Config::RECORD_SIZE; }
    RowCount getPageSizeInRecords() const { return PAGE_SIZE_IN_RECORDS; }
    PageCount getClusterSize() const { return CLUSTER_SIZE; }
    int getMergeFanIn() const { return MERGE_FAN_IN; }
    int getMergeFanOut() const { return MERGE_FAN_OUT; }
    RowCount getMergeFanInRecords() const { return _mergeFanInRec; }
    RowCount getMergeFanOutRecords() const { return _mergeFanOutRec; }
    std::string getReadFilePath() const { return readFilePath; }
    std::string getWriteFilePath() const { return writeFilePath; }
    std::string getName() const { return name; }

    // time calculations in ms
    double getAccessTimeInSec(int nRecords) const;
    int getAccessTimeInMillis(int nRecords) const;
    // double getWriteTimeInMillis(int nRecords) const;

    // file operations
    bool readFrom(const std::string &filePath);
    bool writeTo(const std::string &filePath);
    std::streampos getReadPosition() { return readFile.tellg(); }
    std::streampos getWritePosition() { return writeFile.tellp(); }
    char *readRecords(RowCount *nRecords);
    // RowCount writeRecords(char *data, RowCount nRecords);

    std::vector<Page *> readNext(int nPages);
    void writeNext(std::vector<Page *> pages);
    int readPage(std::ifstream &file, Page *page);

    // cleanup
    void closeRead();
    void closeWrite();
};


// =========================================================
// ------------------------ RunManager ---------------------
// =========================================================

class RunManager {
  private:
    std::string baseDir;
    // std::vector<std::string> runFiles;
    int nextRunIndex = 0;
    RowCount _currSize = 0;
    Storage *storage;

    // helpers
    std::string getNextRunFileName();

  public:
    RunManager(Storage *storage);

    // getters
    std::string getBaseDir() { return baseDir; }
    int getNextRunIndex() { return nextRunIndex; }
    RowCount getCurrSizeInRecords() { return _currSize; }
    std::vector<std::string> getStoredRunsSortedBySize();
    std::vector<std::string> getStoredRunsInfo();

    // operations
    void storeRun(std::vector<Page *> &pages);
    void storeRun(Run &run);
    std::vector<Page *> loadRun(const std::string &runFilename, int pageSize);
    void deleteRun(const std::string &runFilename);

    // validation
    void validateRun(const std::string &runFilename, int pageSize);

    // print
    void printRunFiles();
    void printRunFilesSortedBySize();

    char *repr() {
        std::string repr = storage->getName() + "RunManager: ";
        repr += "dir: " + baseDir + ", ";
        repr += "stored " + std::to_string(nextRunIndex) + " runs, ";
        repr += "used space: " + std::to_string(_currSize) + " out of " +
                std::to_string(storage->getCapacityInRecords()) + " records";
        return strdup(repr.c_str());
    }
};


// ==================================================================
// ------------------------------ Disk ------------------------------
// ==================================================================

class HDD : public Storage {

  private:
    static HDD *instance;
    RunManager *runManager;

  protected:
    HDD(std::string name = "HDD", ByteCount capacity = Config::HDD_SIZE,
        int bandwidth = Config::HDD_BANDWIDTH, double latency = Config::HDD_LATENCY);

  public:
    static HDD *getInstance() {
        if (instance == nullptr) {
            instance = new HDD();
        }
        return instance;
    }

    ~HDD();

    // getters
    RunManager *getRunManager() { return runManager; }
};

// ==================================================================
// ------------------------------ SSD ------------------------------
// ==================================================================

class SSD : public HDD {

  private:
    static SSD *instance;
    RunManager *runManager;
    SSD();

  public:
    static SSD *getInstance() {
        if (instance == nullptr) {
            instance = new SSD();
        }
        return instance;
    }

    // getters
    RunManager *getRunManager() { return runManager; }
    void spillRunsToHDD(HDD *hdd);
};

// ==================================================================
// ------------------------------ DRAM ------------------------------
// ==================================================================

class DRAM : public Storage {

  private:
    // char *buffer;
    static DRAM *instance;
    RowCount _cacheSize;
    // linked list of Records // for loading records
    Record *_head;
    Record *_tail;
    // Current runs in DRAM
    std::vector<Run> _miniruns;

    DRAM();

  public:
    static DRAM *getInstance() {
        if (instance == nullptr) {
            instance = new DRAM();
        }
        return instance;
    }

    ~DRAM() { // delete[] buffer;
    }
    void resetRecords() {
        _head = nullptr;
        _tail = nullptr;
    }
    void loadRecordsToDRAM(char *data, RowCount nRecords);
    /**
     * @brief will mess up the head and tail pointers
     */
    void genMiniRuns(RowCount nRecords);
    void mergeMiniRuns(RunManager *runManager);
};

#endif // _STORAGE_H_
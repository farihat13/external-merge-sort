#ifndef _STORAGE_H_
#define _STORAGE_H_

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
    int PAGE_SIZE_IN_RECORDS = 0; // in records
    int CLUSTER_SIZE = 0;         // in pages
    int MERGE_FAN_IN = 16;        // number of runs to merge at a time
    int MERGE_FAN_OUT = 4;

    // ---- read/write buffer ----
    std::string readFilePath, writeFilePath;
    std::ifstream readFile;
    std::ofstream writeFile;

  public:
    Storage(std::string name, ByteCount capacity, int bandwidth, double latency);

    // setup
    void configure();

    // getters
    ByteCount getCapacityInBytes() const { return CAPACITY_IN_BYTES; }
    RowCount getCapacityInRecords() const { return CAPACITY_IN_BYTES / Config::RECORD_SIZE; }
    ByteCount getPageSizeInBytes() const { return PAGE_SIZE_IN_RECORDS * Config::RECORD_SIZE; }
    int getPageSizeInRecords() const { return PAGE_SIZE_IN_RECORDS; }
    int getClusterSize() const { return CLUSTER_SIZE; }
    int getMergeFanIn() const { return MERGE_FAN_IN; }
    int getMergeFanOut() const { return MERGE_FAN_OUT; }
    std::string getReadFilePath() const { return readFilePath; }
    std::string getWriteFilePath() const { return writeFilePath; }
    std::string getName() const { return name; }

    // file operations
    void readFrom(const std::string &filePath);
    void writeTo(const std::string &filePath);
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
    Storage *storage;

    // helpers
    std::string getNextRunFileName();
    std::vector<std::string> getStoredRunsSortedBySize();
    std::vector<std::string> getStoredRunsInfo();

  public:
    RunManager(Storage *storage);

    // operations
    void storeRun(std::vector<Page *> &pages);
    std::vector<Page *> loadRun(const std::string &runFilename, int pageSize);
    void deleteRun(const std::string &runFilename);

    // validation
    void validateRun(const std::string &runFilename, int pageSize);

    // print
    void printAllRuns();
};


// ==================================================================
// ------------------------------ Disk ------------------------------
// ==================================================================

class HDD : public Storage {

  private:
    static HDD *instance;
    RunManager *runManager;
    HDD();

  public:
    std::fstream fileStream;
    std::string filename;

    static HDD *getInstance() {
        if (instance == nullptr) {
            instance = new HDD();
        }
        return instance;
    }

    ~HDD();

    void write(const char *data, size_t size);
    void read(char *data, size_t size);
    void seek(size_t pos);
};

// ==================================================================
// ------------------------------ DRAM ------------------------------
// ==================================================================

class DRAM : public Storage {

  private:
    char *buffer;
    static DRAM *instance;

    DRAM();

  public:
    static DRAM *getInstance() {
        if (instance == nullptr) {
            instance = new DRAM();
        }
        return instance;
    }

    ~DRAM() { delete[] buffer; }

    char *getBuffer() { return buffer; }
};

#endif // _STORAGE_H_
#ifndef _STORAGE_H_
#define _STORAGE_H_


#include "common.h"
#include "record.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>


// =========================================================
//  -------------------------- Storage ---------------------
// =========================================================

class Storage {
  public:
    std::string name;
    // ---- configurations ----
    long long CAPACITY_IN_BYTES = 0; // in bytes
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

    Storage(std::string name, long long capacity, int bandwidth, double latency)
        : name(name), CAPACITY_IN_BYTES(capacity), BANDWIDTH(bandwidth), LATENCY(latency) {
        printv("Storage: %6s, Capacity %8sBytes,  Bandwidth %6sBytes/s, "
               "Latency %2.6f seconds\n",
               name.c_str(), formatNum(capacity).c_str(), formatNum(bandwidth).c_str(), latency);
        this->configure();
    }

    void configure();

    void readFrom(const std::string &filePath);
    void writeTo(const std::string &filePath);
    std::vector<Page *> readNext(int nPages);
    void writeNext(std::vector<Page *> pages);
    int readPage(std::ifstream &file, Page *page);
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

  public:
    RunManager(Storage *storage) : storage(storage) {
        baseDir = storage->name + "_runs";
        if (!std::filesystem::exists(baseDir)) {
            std::filesystem::create_directory(baseDir);
        }
    }

    std::string getNextRunFileName() {
        std::string filename = baseDir + "/r" + std::to_string(nextRunIndex++) + ".bin";
        return filename;
    }

    std::vector<std::string> getStoredRunsInfo() {
        std::vector<std::string> runFiles;
        for (const auto &entry : std::filesystem::directory_iterator(baseDir)) {
            if (entry.is_regular_file()) {
                runFiles.push_back(entry.path().filename().string());
            }
        }
        std::sort(runFiles.begin(), runFiles.end());
        return runFiles;
    }

    std::vector<std::string> getStoredRunsSortedBySize() {
        std::vector<std::string> runFiles = getStoredRunsInfo();
        std::sort(runFiles.begin(), runFiles.end(),
                  [&](const std::string &a, const std::string &b) {
                      return std::filesystem::file_size(baseDir + "/" + a) <
                             std::filesystem::file_size(baseDir + "/" + b);
                  });
        return runFiles;
    }

    void printAllRuns() {
        printv("All runs in %s:\n", baseDir.c_str());
        for (const auto &file : getStoredRunsInfo()) {
            printv("\t%s\n", file.c_str());
        }
    }

    void deleteRun(const std::string &runFilename) {
        std::string fullPath = baseDir + "/" + runFilename;
        if (std::filesystem::remove(fullPath)) {
            printv("Deleted run file: %s\n", fullPath.c_str());
        } else {
            fprintf(stderr, "Error: Cannot delete run file: %s\n", fullPath.c_str());
        }
    }

    void storeRun(std::vector<Page *> &pages) {
        std::string filename = getNextRunFileName();
        RunWriter writer(filename);
        writer.writeNextPages(pages);
    }

    std::vector<Page *> loadRun(const std::string &runFilename, int pageSize) {
        RunReader reader(runFilename, pageSize);
        return reader.readNextPages(pageSize); // Assume this reads the whole file
    }

    void validateRun(const std::string &runFilename, int pageSize) {
        std::vector<Page *> pages = loadRun(runFilename, pageSize);
        for (auto &page : pages) {
            if (!page->isSorted()) {
                throw std::runtime_error("Run is not sorted: " + runFilename);
            }
        }
        // Clean up pages
        for (auto page : pages) {
            delete page;
        }
    }
};


// =========================================================
// -------------------------- DRAM -------------------------
// =========================================================

// Singleton DRAM class
class DRAM : public Storage {
  private:
    // ---- configurations ----
    long long maxNumRecords;
    int bufferSizeInBytes;
    int numWays = 16;

    // ---- current state ----
    char *data;
    long long nRecords = 0;

    // ---- singleton instance ----
    static DRAM *instance;
    DRAM() : Storage("DRAM", Config::DRAM_SIZE, Config::DRAM_BANDWIDTH, Config::DRAM_LATENCY) {
        this->data = new char[Config::DRAM_SIZE];
        this->maxNumRecords = Config::DRAM_SIZE / Config::RECORD_SIZE;
        this->bufferSizeInBytes = Config::DRAM_BANDWIDTH * Config::DRAM_LATENCY;
        printv("\nDRAM: max %d records, Buffer size %d bytes\n----\n", maxNumRecords,
               bufferSizeInBytes);
    }

  public:
    static DRAM *getInstance() {
        if (instance == nullptr) {
            instance = new DRAM();
        }
        return instance;
    }

    int getMaxNumRecords() { return maxNumRecords; }

    void reset() {
        this->nRecords = 0;
        printv("DRAM: Reset\n");
    }

    /**
     * @brief Read records from file and store in DRAM
     */
    int generateRun(std::ifstream &file, int nRecords);

    friend class SSD;
    friend class HDD;
};


// =========================================================
// -------------------------- SSD --------------------------
// =========================================================

// Singleton SSD class
class SSD : public Storage {
  private:
    // ---- run manager ----
    RunManager *runManager;
    // ---- configurations ----
    long long maxNumRecords;
    int bufferSizeInBytes;
    int maxNumRuns;
    int numWays = 16;
    const std::string filename = "ssd.tmp";

    // ---- current state ----
    long long nRecords = 0;
    int nRuns = 0;
    long long nRecordsPerRun = 0;

    // ---- singleton instance ----
    static SSD *instance;

    SSD() : Storage("SSD", Config::SSD_SIZE, Config::SSD_BANDWIDTH, Config::SSD_LATENCY) {
        // ---- configurations ----
        this->maxNumRecords = Config::SSD_SIZE / Config::RECORD_SIZE;
        this->bufferSizeInBytes = Config::SSD_BANDWIDTH * Config::SSD_LATENCY;
        DRAM *dram = DRAM::getInstance();
        int maxNumRecordsPerRun = dram->getMaxNumRecords();
        this->maxNumRuns = (Config::SSD_SIZE - this->bufferSizeInBytes) /
                           (maxNumRecordsPerRun * Config::RECORD_SIZE);

        // ---- current state ----
        std::ofstream file(this->filename, std::ios::binary);
        file.write("", 1);
        file.close();
        this->reset();
        this->runManager = new RunManager(this);

        printv("\nSSD: max %d records, Buffer size %d bytes,"
               "\n\tMax #records per DRAM_size_run % d"
               "\n\tMax #DRAM_size_runs in SSD %d\n----\n",
               maxNumRecords, bufferSizeInBytes, maxNumRecordsPerRun, maxNumRuns);
    }

  public:
    static SSD *getInstance() {
        if (instance == nullptr) {
            instance = new SSD();
        }
        return instance;
    }

    int getMaxNumRecords() { return maxNumRecords; }
    int getMaxNumRuns() { return maxNumRuns; }

    void printState() {
        printv("SSD: %d records, %d runs, %d recordsPerRun\n", nRecords, nRuns, nRecordsPerRun);
    }

    void reset() {
        this->nRecords = 0;
        this->nRuns = 0;
        this->nRecordsPerRun = 0;
        printv("SSD: Reset\n");
    }

    int read(int address, char *buffer, int nBytes);
    int write(int address, char *buffer, int nBytes);

    /**
     * @brief copy records from DRAM to SSD
     */
    int storeRun();
    int mergeRuns();
    void mergeRunsPlan();

    // void read(int address, char *data, int size);
    // void write(int address, char *data, int size);

    friend class HDD;
};


// =========================================================
// -------------------------- HDD --------------------------
// =========================================================
// Singleton HDD class
class HDD : public Storage {
  private:
    // ---- configurations ----
    const std::string filename = "hdd.tmp";
    // ---- run manager ----
    RunManager *runManager;
    // ---- current state ----
    long long nRecords = 0;
    int nRuns = 0;
    long long nRecordsPerRun = 0;

    // ---- singleton instance ----
    static HDD *instance;

    HDD() : Storage("HDD", Config::HDD_SIZE, Config::HDD_BANDWIDTH, Config::HDD_LATENCY) {
        std::ofstream file(this->filename, std::ios::binary);
        file.write("", 1);
        file.close();
        this->reset();
        this->runManager = new RunManager(this);
    }

  public:
    static HDD *getInstance() {
        if (instance == nullptr) {
            instance = new HDD();
        }
        return instance;
    }

    void printState() {
        printv("HDD State: %d records, %d runs, %d recordsPerRun\n", nRecords, nRuns,
               nRecordsPerRun);
    }

    void reset() {
        this->nRecords = 0;
        this->nRuns = 0;
        this->nRecordsPerRun = 0;
        printv("HDD: Reset\n");
    }

    // int read(int address, char *buffer, int nBytes);
    // int write(int address, char *buffer, int nBytes);
    // int readPage(std::ifstream &file, Page &page);

    /**
     * @brief copy records from SSD to HDD
     */
    int storeRun();
    int mergeRuns();
    int firstPass(int eachRunSizeInRecords);
};


// =========================================================
// --------------------- External Sort ---------------------
// =========================================================


void externalSort();
// void externalSortPlan();

#endif // _STORAGE_H_
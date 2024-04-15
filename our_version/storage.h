#ifndef _STORAGE_H_
#define _STORAGE_H_


#include "common.h"
#include <algorithm>
#include <vector>

// =========================================================
// ------------------------- Page --------------------------
// =========================================================

class Page {
  public:
    std::vector<Record> records;

    Page(int size) {
        records.resize(size);
        for (int i = 0; i < size; i++) {
            records[i].data = new char[Config::RECORD_SIZE];
        }
    }
    ~Page() {
        for (int i = 0; i < records.size(); i++) {
            delete[] records[i].data;
        }
    }
    int sizeInRecords() { return records.size(); }
    int put(int address, std::vector<Record> &v);
};

// =========================================================
//  -------------------------- Storage ---------------------
// =========================================================

class Storage {
  public:
    std::string name;
    // ---- configurations ----
    int BANDWIDTH = 0;               // in MB/s
    double LATENCY = 0;              // in ms
    long long CAPACITY_IN_BYTES = 0; // in bytes

    // ---- calibrable configurations ----
    int PAGE_SIZE_IN_RECORDS = 0; // in records
    int CLUSTER_SIZE = 0;         // in pages
    int MERGE_FAN_IN = 16;        // number of runs to merge at a time
    int MERGE_FAN_OUT = 4;

    Storage(std::string name, long long capacity, int bandwidth,
            double latency) {
        this->name = name;
        this->CAPACITY_IN_BYTES = capacity;
        this->BANDWIDTH = bandwidth;
        this->LATENCY = latency;
        printv("Storage: %s, Capacity %sBytes,  Bandwidth %sBytes/s, "
               "Latency %f seconds\n",
               name.c_str(), formatNum(capacity).c_str(),
               formatNum(bandwidth).c_str(), latency);
        this->configure();
    }

    void configure();
    // virtual int read(int address, char *buffer, int nBytes) = 0;
    // virtual int write(int address, char *buffer, int nBytes) = 0;
    // virtual int storeRun() = 0;
    // virtual int mergeRuns() = 0;
    // virtual void reset() = 0;
    // virtual void printState() = 0;
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
    DRAM()
        : Storage("DRAM", Config::DRAM_SIZE, Config::DRAM_BANDWIDTH,
                  Config::DRAM_LATENCY) {
        this->data = new char[Config::DRAM_SIZE];
        this->maxNumRecords = Config::DRAM_SIZE / Config::RECORD_SIZE;
        this->bufferSizeInBytes = Config::DRAM_BANDWIDTH * Config::DRAM_LATENCY;
        printv("\nDRAM: max %d records, Buffer size %d bytes\n----\n",
               maxNumRecords, bufferSizeInBytes);
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

    SSD()
        : Storage("SSD", Config::SSD_SIZE, Config::SSD_BANDWIDTH,
                  Config::SSD_LATENCY) {
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

        printv("\nSSD: max %d records, Buffer size %d bytes,"
               "\n\tMax #records per DRAM_size_run % d"
               "\n\tMax #DRAM_size_runs in SSD %d\n----\n",
               maxNumRecords, bufferSizeInBytes, maxNumRecordsPerRun,
               maxNumRuns);
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
        printv("SSD: %d records, %d runs, %d recordsPerRun\n", nRecords, nRuns,
               nRecordsPerRun);
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
    // ---- current state ----
    long long nRecords = 0;
    int nRuns = 0;
    long long nRecordsPerRun = 0;

    // ---- singleton instance ----
    static HDD *instance;

    HDD()
        : Storage("HDD", Config::HDD_SIZE, Config::HDD_BANDWIDTH,
                  Config::HDD_LATENCY) {
        std::ofstream file(this->filename, std::ios::binary);
        file.write("", 1);
        file.close();
        this->reset();
    }

  public:
    static HDD *getInstance() {
        if (instance == nullptr) {
            instance = new HDD();
        }
        return instance;
    }

    void printState() {
        printv("HDD State: %d records, %d runs, %d recordsPerRun\n", nRecords,
               nRuns, nRecordsPerRun);
    }

    void reset() {
        this->nRecords = 0;
        this->nRuns = 0;
        this->nRecordsPerRun = 0;
        printv("HDD: Reset\n");
    }

    int read(int address, char *buffer, int nBytes);
    int write(int address, char *buffer, int nBytes);
    int readPage(std::ifstream &file, Page &page);

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
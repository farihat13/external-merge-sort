#ifndef _STORAGE_H_
#define _STORAGE_H_


#include "common.h"


// =========================================================
// -------------------------- DRAM -------------------------
// =========================================================

// Singleton DRAM class
class DRAM {
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
    DRAM() {
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
class SSD {
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

    SSD() {
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
class HDD {
  private:
    // ---- configurations ----
    const std::string filename = "hdd.tmp";
    // ---- current state ----
    long long nRecords = 0;
    int nRuns = 0;
    long long nRecordsPerRun = 0;


    HDD() {
        std::ofstream file(this->filename, std::ios::binary);
        file.write("", 1);
        file.close();
        this->reset();
        printv("\nHDD: Infinite size\n----\n");
    }

  public:
    // delete copy constructor and copy assignment operator to prevent copies
    HDD(HDD const &) = delete;
    void operator=(HDD const &) = delete;

    static HDD &getInstance() {
        static HDD instance;
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

    /**
     * @brief copy records from SSD to HDD
     */
    int storeRun();
    int mergeRuns();
    void externalSort();
    void externalSortPlan();
};

#endif // _STORAGE_H_
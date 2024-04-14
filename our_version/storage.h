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
    int maxNumRecords;
    int bufferSizeInBytes;

    // ---- current state ----
    char *data;
    int nRecords = 0;

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
    int maxNumRecords;
    int bufferSizeInBytes;
    int maxNumRuns;


    // ---- current state ----
    char *data;
    int nRecords = 0;
    int nRuns = 0;

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
        this->data = new char[Config::SSD_SIZE];
        this->nRecords = 0;
        this->nRuns = 0;

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

    void reset() {
        this->nRecords = 0;
        this->nRuns = 0;
        printv("SSD: Reset\n");
    }

    /**
     * @brief copy records from DRAM to SSD
     */
    int storeRun();

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
    // ---- current state ----
    int nRecords = 0;


    HDD() {
        this->nRecords = 0;
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

    void externalSort();
};

#endif // _STORAGE_H_
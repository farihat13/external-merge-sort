#ifndef _STORAGE_H_
#define _STORAGE_H_


#include "common.h"


// =========================================================
// -------------------------- DRAM -------------------------
// =========================================================

// Singleton DRAM class
class DRAM {
  private:
    static DRAM *instance;
    DRAM() {
        this->data = new char[Config::DRAM_SIZE];
        this->nRecords = Config::DRAM_SIZE / Config::RECORD_SIZE;
    }

  public:
    char *data;
    int nRecords;

    static DRAM *getInstance() {
        if (instance == nullptr) {
            instance = new DRAM();
        }
        return instance;
    }

    /**
     * @brief Read records from file and store in DRAM
     */
    int generateRun(std::ifstream &file, int nRecords);
};


// =========================================================
// -------------------------- SSD --------------------------
// =========================================================

// Singleton SSD class
class SSD {
  private:
    static SSD *instance;
    SSD() { data = new char[Config::SSD_SIZE]; }

  public:
    char *data;
    static SSD *getInstance() {
        if (instance == nullptr) {
            instance = new SSD();
        }
        return instance;
    }


    int generateRun();

    // void read(int address, char *data, int size);
    // void write(int address, char *data, int size);
};


// =========================================================
// -------------------------- HDD --------------------------
// =========================================================
// Singleton HDD class
class HDD {
  private:
    char *data;
    HDD() { data = new char[Config::HDD_SIZE]; }

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
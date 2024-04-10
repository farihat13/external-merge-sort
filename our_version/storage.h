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
    char *data;
    DRAM() { data = new char[Config::DRAM_SIZE]; }

  public:
    static DRAM *getInstance() {
        if (instance == nullptr) {
            instance = new DRAM();
        }
        return instance;
    }

    // void read(int address, char *data, int size);
    // void write(int address, char *data, int size);
};


// =========================================================
// -------------------------- SSD --------------------------
// =========================================================

// Singleton SSD class
class SSD {
  private:
    static SSD *instance;
    char *data;
    SSD() { data = new char[Config::SSD_SIZE]; }

  public:
    static SSD *getInstance() {
        if (instance == nullptr) {
            instance = new SSD();
        }
        return instance;
    }

    // void read(int address, char *data, int size);
    // void write(int address, char *data, int size);
};


// =========================================================
// -------------------------- HDD --------------------------
// =========================================================
// Singleton HDD class

class HDD {
  private:
    static HDD *instance;
    char *data;
    HDD() { data = new char[Config::HDD_SIZE]; }

  public:
    static HDD *getInstance() {
        if (instance == nullptr) {
            instance = new HDD();
        }
        return instance;
    }

    // void read(int address, char *data, int size);
    // void write(int address, char *data, int size);
};

#endif // _STORAGE_H_
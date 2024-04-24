#ifndef _STORAGE_TYPES_H_
#define _STORAGE_TYPES_H_

#include "Losertree.h"


// ==================================================================
// ------------------------------ Disk ------------------------------
// ==================================================================

class HDD : public Storage {
  private:
    static HDD *instance;

    // ---- only needed for mergeHDDRuns ----
    int setupMergeStateInSSDAndDRAM();
    std::pair<std::vector<RunStreamer *>, RowCount> loadRunfilesToDRAM(size_t fanIn);

  protected:
    HDD(std::string name = DISK_NAME, ByteCount capacity = Config::HDD_CAPACITY,
        int bandwidth = Config::HDD_BANDWIDTH, double latency = Config::HDD_LATENCY);

  public:
    static HDD *getInstance() {
        if (instance == nullptr) { instance = new HDD(); }
        return instance;
    }

    ~HDD() {
        if (runManager != nullptr) { delete runManager; }
    }

    // void setupMergeState(RowCount outputDevicePageSize, int fanIn);
    // int setupMergeStateForMiniruns(RowCount outputDevicePageSize);
    void mergeHDDRuns();
    RowCount storeRun(Run &run);
    // ---- helper functions ----
    void printStates(std::string where);
};

// ==================================================================
// ------------------------------ SSD ------------------------------
// ==================================================================

class SSD : public HDD {

  private:
    static SSD *instance;
    SSD();

  public:
    static SSD *getInstance() {
        if (instance == nullptr) { instance = new SSD(); }
        return instance;
    }
    ~SSD() {}
    void setupMergeState(RowCount outputDevicePageSize, int fanIn);
    void mergeSSDRuns(HDD *outputDevice);
};

// ==================================================================
// ------------------------------ DRAM ------------------------------
// ==================================================================

class DRAM : public Storage {

  private:
    // char *buffer;
    static DRAM *instance;

    // ---- internal state for generating mini-runs ----
    Record *_head;              // linked list of Records for loading records
    std::vector<Run> _miniruns; // Current runs in DRAM

    DRAM();

  public:
    static DRAM *getInstance() {
        if (instance == nullptr) { instance = new DRAM(); }
        return instance;
    }

    ~DRAM() { // delete[] buffer;
    }
    void setupMergeState(RowCount outputDevicePageSize, int fanIn);
    int setupMergeStateForMiniruns(RowCount outputDevicePageSize);

    void reset() {
        _head = nullptr;
        _miniruns.clear();
        this->resetAllFilledSpace();
    }
    // void loadRecordsToDRAM(char *data, RowCount nRecords);
    RowCount loadInput(RowCount nRecords);
    /**
     * @brief will mess up the head and tail pointers
     */
    void genMiniRuns(RowCount nRecords);
    void mergeMiniRuns(HDD *outputStorage);
};


// =============================================================================
// ------------------------------ CommonFunctions ------------------------------
// =============================================================================


int getDRAMAccessTime(RowCount nRecords);
int getSSDAccessTime(RowCount nRecords);
int getHDDAccessTime(RowCount nRecords);

#endif // _STORAGE_TYPES_H_
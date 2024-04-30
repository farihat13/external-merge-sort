#ifndef _STORAGE_TYPES_H_
#define _STORAGE_TYPES_H_

#include "Losertree.h"
#include "RunStreamer.h"


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
    /**
     * @brief Get the singleton instance of HDD.
     */
    static HDD *getInstance() {
        if (instance == nullptr) { instance = new HDD(); }
        return instance;
    }

    /**
     * @brief Delete the singleton instance of HDD.
     */
    static void deleteInstance() {
        if (instance != nullptr) {
            delete instance;
            instance = nullptr;
        }
    }

    /**
     * @brief Destructor for HDD.
     */
    ~HDD() {
        if (runManager != nullptr) {
            delete runManager;
            runManager = nullptr;
        }
    }

    /**
     * @brief merge the runs in the HDD and SSD together to a single run.
     */
    void mergeHDDRuns();

    /**
     * @brief Store the run in a new runfile using a RunWriter.
     * @return Number of records stored.
     */
    RowCount storeRun(Run *run);

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
    /**
     * @brief Get the singleton instance of SSD.
     */
    static SSD *getInstance() {
        if (instance == nullptr) { instance = new SSD(); }
        return instance;
    }

    /**
     * @brief Delete the singleton instance of SSD.
     */
    static void deleteInstance() {
        if (instance != nullptr) {
            delete instance;
            instance = nullptr;
        }
    }

    /**
     * @brief Destructor for SSD.
     */
    ~SSD() {
        if (runManager != nullptr) {
            delete runManager;
            runManager = nullptr;
        }
    }

    /**
     * @brief Setup the merge state for SSD.
     */
    void setupMergeState(RowCount outputDevicePageSize, int fanIn);

    /**
     * @brief Store the run in a new runfile using a RunWriter.
     */
    void freeSpaceBySpillingRunfiles();

    /**
     * @brief Merge the runs in the SSD and store the final run in HDD.
     */
    void mergeSSDRuns(HDD *outputDevice);
};

// ==================================================================
// ------------------------------ DRAM ------------------------------
// ==================================================================

class DRAM : public Storage {

  private:
    static DRAM *instance;

    // ---- internal state for generating mini-runs ----
    Record *_head; // linked list of Records for loading records
    DRAM();

  public:
    /**
     * @brief Get the singleton instance of DRAM.
     */
    static DRAM *getInstance() {
        if (instance == nullptr) { instance = new DRAM(); }
        return instance;
    }

    /**
     * @brief Delete the singleton instance of DRAM.
     */
    static void deleteInstance() {
        if (instance != nullptr) {
            delete instance;
            instance = nullptr;
        }
    }

    /**
     * @brief Destructor for DRAM.
     */
    ~DRAM() {}

    /**
     * @brief Setup the merge state for DRAM.
     */
    void setupMergeState(RowCount outputDevicePageSize, int fanIn);

    /**
     * @brief Setup the merge state for DRAM for generating mini-runs.
     * Since, fanIn is not provided, it will use MERGE_FAN_OUT as fanOut
     * and calculate totalInputClusterSize based on outputDevicePageSize
     * @return the fanOut value used
     */
    int setupMergeStateForMiniruns(RowCount outputDevicePageSize);

    /**
     * @brief Reset the DRAM state.
     */
    void reset() {
        _head = nullptr;
        this->resetAllFilledSpace();
    }

    /**
     * @brief Load nRecords from input file to DRAM.
     */
    RowCount loadInput(RowCount nRecords);

    /**
     * @brief Generate mini-runs from the loaded records.
     * Merge the mini-runs and store the final run in outputStorage.
     * @param nRecords Number of records to generate mini-runs.
     */
    void genMiniRuns(RowCount nRecords, HDD *outputStorage);
};


// =============================================================================
// ------------------------------ CommonFunctions ------------------------------
// =============================================================================


double getDRAMAccessTime(RowCount nRecords);
double getSSDAccessTime(RowCount nRecords);
double getHDDAccessTime(RowCount nRecords);

#endif // _STORAGE_TYPES_H_
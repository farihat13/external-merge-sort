#include "Storage.h"

// ==================================================================
// ------------------------------ Disk ------------------------------
// ==================================================================

class HDD : public Storage {
  private:
    static HDD *instance;

  protected:
    HDD(std::string name = "HDD", ByteCount capacity = Config::HDD_CAPACITY,
        int bandwidth = Config::HDD_BANDWIDTH, double latency = Config::HDD_LATENCY);

  public:
    static HDD *getInstance() {
        if (instance == nullptr) {
            instance = new HDD();
        }
        return instance;
    }

    ~HDD() {
        if (runManager != nullptr) {
            delete runManager;
        }
    }

    RowCount storeRun(Run &run) {
        if (getTotalEmptySpaceInRecords() < run.getSize()) {
            printvv("ERROR: Run size %lld exceeds storage empty space\n", run.getSize());
            printvv("%s\n", reprUsageDetails().c_str());
            throw std::runtime_error(this->getName() + " is full, cannot store run");
        }
        std::string filename = runManager->getNextRunFileName();
        RunWriter writer(filename);
        RowCount nRecords = writer.writeNextRun(run);
        if (nRecords != run.getSize()) {
            printvv("ERROR: Writing %lld records, expected %lld\n", nRecords, run.getSize());
            throw std::runtime_error("Error: Writing run to file");
        }
        runManager->addRunFile(filename, nRecords);
        _filled += nRecords;
        // this->storeMore(nRecords);
        return nRecords;
    }
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
        if (instance == nullptr) {
            instance = new SSD();
        }
        return instance;
    }
    ~SSD() {}
    void spillRunsToHDD(HDD *hdd);
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
        if (instance == nullptr) {
            instance = new DRAM();
        }
        return instance;
    }

    ~DRAM() { // delete[] buffer;
    }
    void reset() {
        _head = nullptr;
        _miniruns.clear();
        this->resetAllFilledSpace();
    }
    void loadRecordsToDRAM(char *data, RowCount nRecords);
    /**
     * @brief will mess up the head and tail pointers
     */
    void genMiniRuns(RowCount nRecords);
    void mergeMiniRuns(HDD *outputStorage);
};

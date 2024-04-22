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
// ------------------------ RunManager ---------------------
// =========================================================

class RunManager {
  private:
    std::string baseDir;
    std::vector<std::pair<std::string, RowCount>> runFiles;
    int nextRunIndex = 0; // next run id
    RowCount totalRecords = 0;

    std::vector<std::string> getRunInfoFromDir();

  public:
    RunManager(std::string deviceName);
    ~RunManager();

    // setters
    void addRunFile(std::string filename, RowCount nRecords) {
        runFiles.push_back({filename, nRecords});
        totalRecords += nRecords;
    }

    // getters
    std::string getBaseDir() { return baseDir; }
    RowCount getTotalRecords() { return totalRecords; }
    std::string getNextRunFileName();
    std::vector<std::pair<std::string, RowCount>> &getStoredRunsSortedBySize();

    std::string repr() {
        std::string repr = "\n\t" + baseDir + "RunManager: " + "stored " +
                           std::to_string(runFiles.size()) + " runs, ";
        return repr;
    }
}; // class RunManager


// =========================================================
//  -------------------------- Storage ---------------------
// =========================================================

class Storage {
    std::string name;
    // ---- provided configurations ----
    ByteCount CAPACITY_IN_BYTES = 0; // in bytes
    int BANDWIDTH = 0;               // in MB/s
    double LATENCY = 0;              // in ms
    // ---- calibrable configurations ----
    RowCount PAGE_SIZE_IN_RECORDS = 0; // in records
    PageCount CLUSTER_SIZE = 0;        // in pages
    int MERGE_FAN_IN = 30;             // #runs to merge at a time, or #input_clusters
    int MERGE_FAN_OUT = 2;             // #output_clusters
    RowCount MERGE_FANIN_IN_RECORDS;   // total #records to merge at a time per input cluster
    RowCount MERGE_FANOUT_IN_RECORDS;  // total #records that can be stored in output clusters
    // ---- read/write buffer ----
    std::string readFilePath, writeFilePath;
    std::ifstream readFile;
    std::ofstream writeFile;

  protected:
    // run manager
    RunManager *runManager; // only for HDD and SSD
    // ---- internal state ----
    RowCount _filled = 0; // updated by RunManager
    // used for merging
    RowCount _effectiveClusterSize = 0;
    RowCount _filledInputClusters = 0;
    RowCount _filledOutputClusters = 0;
    RowCount _totalSpaceInInputClusters = 0;  // setup before merging
    RowCount _totalSpaceInOutputClusters = 0; // setup before merging

  public:
    Storage(std::string name, ByteCount capacity, int bandwidth, double latency);

    // setup
    void configure();

    // ---- getters ----
    std::string getName() const { return name; }
    ByteCount getCapacityInBytes() const { return CAPACITY_IN_BYTES; }
    RowCount getCapacityInRecords() const { return CAPACITY_IN_BYTES / Config::RECORD_SIZE; }
    ByteCount getPageSizeInBytes() const { return PAGE_SIZE_IN_RECORDS * Config::RECORD_SIZE; }
    RowCount getPageSizeInRecords() const { return PAGE_SIZE_IN_RECORDS; }
    PageCount getClusterSizeInPages() const { return CLUSTER_SIZE; }
    int getMergeFanIn() const { return MERGE_FAN_IN; }
    int getMergeFanOut() const { return MERGE_FAN_OUT; }
    RowCount getMergeFanInRecords() const { return MERGE_FANIN_IN_RECORDS; }
    RowCount getMergeFanOutRecords() const { return MERGE_FANOUT_IN_RECORDS; }
    std::string getReadFilePath() const { return readFilePath; }
    std::string getWriteFilePath() const { return writeFilePath; }
    // time calculations in ms
    double getAccessTimeInSec(RowCount nRecords) const {
        return this->LATENCY + (nRecords * Config::RECORD_SIZE) / this->BANDWIDTH;
    }
    int getAccessTimeInMillis(RowCount nRecords) const {
        return (int)(this->getAccessTimeInSec(nRecords) * 1000);
    }

    // ---- state ----
    RowCount getTotalEmptySpaceInRecords() {
        return getCapacityInRecords() - getTotalFilledSpaceInRecords();
    }
    RowCount getTotalFilledSpaceInRecords() {
        return _filled + _filledInputClusters + _filledOutputClusters;
    }
    void storeMore(RowCount nRecords) { _filled += nRecords; }
    void freeMore(RowCount nRecords) { _filled -= nRecords; }
    void resetAllFilledSpace() {
        _filled = 0;
        _filledInputClusters = 0;
        _filledOutputClusters = 0;
    }
    std::string reprUsageDetails() {
        std::string state = "\n\t" + this->name + " usage details: ";
        state += "\n\t\t_filled: " + std::to_string(_filled) + " records";
        state += "\n\t\tinputcluster: " + std::to_string(_filledInputClusters) + " out of " +
                 std::to_string(_totalSpaceInInputClusters) + " records, ";
        state += "\n\t\toutputcluster: " + std::to_string(_filledOutputClusters) + " out of " +
                 std::to_string(_totalSpaceInOutputClusters) + " records";
        state +=
            "\n\t\teffective cluster size: " + std::to_string(_effectiveClusterSize) + " records";
        state +=
            "\n\t\ttotal filled: " + std::to_string(getTotalFilledSpaceInRecords()) + " records";
        state += "\n\t\ttotal capacity: " + std::to_string(getCapacityInRecords()) + " records";
        state += "\n\t\ttotal empty space: " + std::to_string(getTotalEmptySpaceInRecords()) +
                 " records";
        state += "\n\t\t" + runManager->repr();
        return state;
    }
    // ---- merging state ----
    void setupMergingState(RowCount totalInputRecords, RowCount totalOutputRecords,
                           RowCount effectiveClusterSize) {
        _totalSpaceInInputClusters = totalInputRecords;
        _totalSpaceInOutputClusters = totalOutputRecords;
        _effectiveClusterSize = effectiveClusterSize;
        _filledInputClusters = 0;
        _filledOutputClusters = 0;
    }
    void fillInputCluster(RowCount nRecords) { _filledInputClusters += nRecords; }
    void fillOutputCluster(RowCount nRecords) { _filledOutputClusters += nRecords; }
    void freeInputCluster(RowCount nRecords) { _filledInputClusters -= nRecords; }
    void freeOutputCluster(RowCount nRecords) { _filledOutputClusters -= nRecords; }
    void getMergingState(RowCount &filledInputClusters, RowCount &filledOutputClusters) {
        filledInputClusters = _filledInputClusters;
        filledOutputClusters = _filledOutputClusters;
    }
    void getEmptySpaceInClusters(RowCount &emptyInputClusters, RowCount &emptyOutputClusters) {
        emptyInputClusters = _totalSpaceInInputClusters - _filledInputClusters;
        emptyOutputClusters = _totalSpaceInOutputClusters - _filledOutputClusters;
    }
    RowCount getEmptySpaceInInputClusters() {
        return _totalSpaceInInputClusters - _filledInputClusters;
    }
    RowCount getEmptySpaceInOutputClusters() {
        return _totalSpaceInOutputClusters - _filledOutputClusters;
    }
    RowCount getFilledSpaceInInputClusters() { return _filledInputClusters; }
    RowCount getFilledSpaceInOutputClusters() { return _filledOutputClusters; }
    RowCount getFilledSpace() { return _filledInputClusters + _filledOutputClusters; }

    // ---- file operations ----
    bool readFrom(const std::string &filePath);
    bool writeTo(const std::string &filePath);
    std::streampos getReadPosition() { return readFile.tellg(); }
    // std::streampos getWritePosition() { return writeFile.tellp(); }
    char *readRecords(RowCount *nRecords);
    // RowCount writeRecords(char *data, RowCount nRecords);
    // cleanup
    void closeRead();
    void closeWrite();

}; // class Storage


// =========================================================
// ----------------------- RunStreamer ---------------------
// =========================================================


/**
 * @brief RunStreamer is a class to stream records from a run
 * It can be used to stream records from a run in memory or from a file
 * If the run is in memory, it will stream records from the run
 * If the run is on disk, it will stream records from the file
 */
class RunStreamer {
  private:
    Record *currentRecord;
    // if reader is not null, it will stream records from file
    // otherwise, it will stream records until currentRecord->next is null
    RunReader *reader;
    Storage *device;
    Page *currentPage;
    int readAhead = 1;
    // read ahead pages
    RowCount readAheadPages(int nPages);

  public:
    RunStreamer(Run *run);
    RunStreamer(RunReader *reader, Storage *device, int readAhead = 1);

    // getters
    Record *getCurrRecord() { return currentRecord; }
    Record *moveNext();

    // default comparison based on first 8 bytes of data
    bool operator<(const RunStreamer &other) const { return *currentRecord < *other.currentRecord; }
    bool operator>(const RunStreamer &other) const { return *currentRecord > *other.currentRecord; }
    // equality comparison based on all bytes of data
    bool operator==(const RunStreamer &other) const {
        return *currentRecord == *other.currentRecord;
    }

    char *repr() { return currentRecord->reprKey(); }
}; // class RunStreamer


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


#endif // _STORAGE_H_
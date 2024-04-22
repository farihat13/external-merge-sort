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

#define DRAM_NAME "DRAM"
#define DISK_NAME "HDD"
#define SSD_NAME "SSD"

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
        std::string repr =
            baseDir + "RunManager: " + "stored " + std::to_string(runFiles.size()) + " runs, ";
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
    int MERGE_FAN_IN = 45;             // #runs to merge at a time, or #input_clusters
    int MERGE_FAN_OUT = 5;             // #output_clusters
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

    // setup
    Storage(std::string name, ByteCount capacity, int bandwidth, double latency);
    void configure();

  public:
    // -------------------------------- getters --------------------------------
    // configurations
    std::string getName() const { return name; }
    RowCount getCapacityInRecords() const { return CAPACITY_IN_BYTES / Config::RECORD_SIZE; }
    RowCount getPageSizeInRecords() const { return PAGE_SIZE_IN_RECORDS; }
    RowCount getMergeFanInRecords() const { return MERGE_FANIN_IN_RECORDS; }
    RowCount getMergeFanOutRecords() const { return MERGE_FANOUT_IN_RECORDS; }
    int getMergeFanIn() const { return MERGE_FAN_IN; }
    int getMergeFanOut() const { return MERGE_FAN_OUT; }
    PageCount getClusterSize() const { return CLUSTER_SIZE; }
    // file I/O
    std::string getReadFilePath() const { return readFilePath; }
    std::string getWriteFilePath() const { return writeFilePath; }
    // time calculations in ms
    double getAccessTimeInSec(RowCount nRecords) const {
        return this->LATENCY + (nRecords * Config::RECORD_SIZE) / this->BANDWIDTH;
    }
    int getAccessTimeInMillis(RowCount nRecords) const {
        return (int)(this->getAccessTimeInSec(nRecords) * 1000);
    }
    // usage details
    RowCount getTotalEmptySpaceInRecords() {
        return getCapacityInRecords() - getTotalFilledSpaceInRecords();
    }
    RowCount getTotalFilledSpaceInRecords() {
        return _filled + _filledInputClusters + _filledOutputClusters;
    }
    // merging states
    RowCount getEffectiveClusterSize() { return _effectiveClusterSize; }
    RowCount getTotalSpaceInInputClusters() { return _totalSpaceInInputClusters; }
    RowCount getTotalSpaceInOutputClusters() { return _totalSpaceInOutputClusters; }
    RowCount getEmptySpaceInInputClusters() {
        return _totalSpaceInInputClusters - _filledInputClusters;
    }
    RowCount getEmptySpaceInOutputClusters() {
        return _totalSpaceInOutputClusters - _filledOutputClusters;
    }
    RowCount getFilledSpaceInInputClusters() { return _filledInputClusters; }
    RowCount getFilledSpaceInOutputClusters() { return _filledOutputClusters; }


    // -------------------------------- setters --------------------------------
    // merging states
    void storeMore(RowCount nRecords) { _filled += nRecords; }
    void freeMore(RowCount nRecords) { _filled -= nRecords; }
    void resetAllFilledSpace() {
        _filled = 0;
        _filledInputClusters = 0;
        _filledOutputClusters = 0;
    }
    virtual void setupMergeState(RowCount outputDevicePageSize, int fanIn) = 0;
    void resetMergeState() {
        _effectiveClusterSize = 0;
        _filledInputClusters = 0;
        _filledOutputClusters = 0;
        _totalSpaceInInputClusters = 0;
        _totalSpaceInOutputClusters = 0;
    }
    /**
     * @brief Setup merging state for the storage device (used for merging miniruns)
     * Since, fanIn is not provided, it will use MERGE_FAN_OUT as fanOut
     * and calculate totalInputClusterSize based on outputDevicePageSize
     * NOTE: the _effectiveClusterSize will be set to -1, don't use it
     * @return the fanOut value used
     */
    virtual int setupMergeStateForMiniruns(RowCount outputDevicePageSize) = 0;
    void fillInputCluster(RowCount nRecords) { _filledInputClusters += nRecords; }
    void fillOutputCluster(RowCount nRecords) { _filledOutputClusters += nRecords; }
    void freeInputCluster(RowCount nRecords) { _filledInputClusters -= nRecords; }
    void freeOutputCluster(RowCount nRecords) { _filledOutputClusters -= nRecords; }


    // ---------------------------- file operations ----------------------------
    bool readFrom(const std::string &filePath);
    bool writeTo(const std::string &filePath);
    std::streampos getReadPosition() { return readFile.tellg(); }
    // std::streampos getWritePosition() { return writeFile.tellp(); }
    char *readRecords(RowCount *nRecords);
    // RowCount writeRecords(char *data, RowCount nRecords);
    // cleanup
    void closeRead();
    void closeWrite();

    // ---------------------------- run management ----------------------------
    RunWriter *getRunWriter();
    void closeWriter(RunWriter *writer);

    // ---------------------------- printing -----------------------------------
    std::string reprUsageDetails();

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


#endif // _STORAGE_H_
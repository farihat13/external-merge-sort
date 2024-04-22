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
    // ------------------------ configurations ---------------------------------
    std::string getName() const { return name; }
    RowCount getCapacityInRecords() const { return CAPACITY_IN_BYTES / Config::RECORD_SIZE; }
    RowCount getPageSizeInRecords() const { return PAGE_SIZE_IN_RECORDS; }
    RowCount getMergeFanInRecords() const { return MERGE_FANIN_IN_RECORDS; }
    RowCount getMergeFanOutRecords() const { return MERGE_FANOUT_IN_RECORDS; }
    int getMergeFanIn() const { return MERGE_FAN_IN; }
    int getMergeFanOut() const { return MERGE_FAN_OUT; }
    PageCount getClusterSize() const { return CLUSTER_SIZE; }


    // ----------------------------- time calculations -------------------------
    double getAccessTimeInSec(RowCount nRecords) const {
        return this->LATENCY + (nRecords * Config::RECORD_SIZE) / this->BANDWIDTH;
    }
    int getAccessTimeInMillis(RowCount nRecords) const {
        return (int)(this->getAccessTimeInSec(nRecords) * 1000);
    }
    // ----------------------------- space calculations ------------------------
    // getters
    RowCount getTotalEmptySpaceInRecords() {
        return getCapacityInRecords() - getTotalFilledSpaceInRecords();
    }
    RowCount getTotalFilledSpaceInRecords() {
        return _filled + _filledInputClusters + _filledOutputClusters;
    }
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
    // setters
    void fillMore(RowCount nRecords) {
        if (_filled + nRecords > getCapacityInRecords()) {
            throw std::runtime_error("ERROR: filling more records than capacity in " + this->name);
        }
        _filled += nRecords;
    }
    void freeSome(RowCount nRecords) {
        if (_filled - nRecords < 0) {
            throw std::runtime_error("ERROR: freeing more records than filled in " + this->name);
        }
        _filled -= nRecords;
    }
    void fillInputCluster(RowCount nRecords) { _filledInputClusters += nRecords; }
    void fillOutputCluster(RowCount nRecords) { _filledOutputClusters += nRecords; }
    void freeInputCluster(RowCount nRecords) { _filledInputClusters -= nRecords; }
    void freeOutputCluster(RowCount nRecords) { _filledOutputClusters -= nRecords; }
    void resetAllFilledSpace() {
        _filled = 0;
        _filledInputClusters = 0;
        _filledOutputClusters = 0;
    }
    // merging states
    void resetMergeState() {
        _effectiveClusterSize = 0;
        _filledInputClusters = 0;
        _filledOutputClusters = 0;
        _totalSpaceInInputClusters = 0;
        _totalSpaceInOutputClusters = 0;
    }
    virtual void setupMergeState(RowCount outputDevicePageSize, int fanIn) = 0;
    /**
     * @brief Setup merging state for dram (used for merging miniruns)
     */
    virtual int setupMergeStateForMiniruns(RowCount outputDevicePageSize) = 0;

    // --------------------------- FILE I/O ------------------------------------
    std::string getReadFilePath() const { return readFilePath; }
    std::string getWriteFilePath() const { return writeFilePath; }
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
    RowCount writeNextChunk(RunWriter *writer, Run &run);
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
    Storage *fromDevice;
    Storage *toDevice;
    Page *currentPage;
    PageCount readAhead = 1;
    // read ahead pages
    RowCount readAheadPages(int nPages);

  public:
    RunStreamer(Run *run);
    RunStreamer(RunReader *reader, Storage *fromDevice, Storage *toDevice, PageCount readAhead = 1);

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
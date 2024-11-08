#ifndef _STORAGE_H_
#define _STORAGE_H_


#include "Record.h"
#include "config.h"
#include "defs.h"
#include <cstddef>
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


/**
 * @brief RunManager class to manage runs in a storage device
 */
class RunManager {
  private:
    std::string baseDir;
    std::vector<std::pair<std::string, RowCount>> runFiles;
    int nextRunIndex = 0; // next run id
    RowCount totalRecords = 0;

    /**
     * @brief reads the run files from the baseDir and returns the list of run files.
     */
    std::vector<std::string> getRunInfoFromDir();

  public:
    /**
     * @brief RunManager class to manage runs in a storage device.
     * It creates a directory for the device runs.
     * If the directory exists, it deletes all run files in the directory.
     * @param deviceName
     */
    RunManager(std::string deviceName);

    /**
     * @brief warns the user if all run files are not deleted
     */
    ~RunManager();

    // Setters
    void addRunFile(std::string filename, RowCount nRecords) {
        runFiles.push_back({filename, nRecords});
        totalRecords += nRecords;
    }

    /**
     * @brief removes the run file from the runFiles list.
     * It also updates the totalRecords count.
     */
    bool removeRunFile(std::string filename) {
        // Find the run file
        auto it = std::find_if(
            runFiles.begin(), runFiles.end(),
            [filename](const std::pair<std::string, RowCount> &p) { return p.first == filename; });

        if (it != runFiles.end()) {
            // Remove the run file and update the total records count
            totalRecords -= it->second;
            runFiles.erase(it);
            return true;
        }
        return false;
    }

    // Getters
    std::string getBaseDir() { return baseDir; }
    RowCount getTotalRecords() { return totalRecords; }
    std::string getNextRunFileName();
    std::vector<std::pair<std::string, RowCount>> &getStoredRunsSortedBySize();

    std::string repr() {
        std::string repr = baseDir + "RunManager: " + "stored " + std::to_string(runFiles.size()) +
                           " runs, total records: " + std::to_string(totalRecords);
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
    int MAX_MERGE_FAN_IN = 95;         // #runs to merge at a time, or #input_clusters
    int MAX_MERGE_FAN_OUT = 5;         // #output_clusters
    // ---- read/write buffer ----
    std::string readFilePath;
    std::ifstream readFile;

  protected:
    PageCount CLUSTER_SIZE = 0;       // in pages
    RowCount MERGE_FANIN_IN_RECORDS;  // total #records to merge at a time per input cluster
    RowCount MERGE_FANOUT_IN_RECORDS; // total #records that can be stored in output clusters
    // run manager
    RunManager *runManager = nullptr; // only for HDD and SSD
    // ---- internal state ----
    RowCount _filled = 0; // updated by RunManager
    // used for merging
    RowCount _effectiveClusterSize = 0;
    RowCount _filledInputClusters = 0;
    RowCount _filledOutputClusters = 0;
    RowCount _totalSpaceInInputClusters = 0;  // setup before merging
    RowCount _totalSpaceInOutputClusters = 0; // setup before merging
    // ----- spill session ----
    // assumption: spill amount will always be less than capacity
    Storage *spillTo = nullptr; // used for merging
    RunWriter *spillWriter = nullptr;

    /**
     * @brief Constructor for the Storage class
     */
    Storage(std::string name, ByteCount capacity, int bandwidth, double latency);

    /**
     * @brief Configure the storage device based on the provided configurations.
     * - Calculate the page size in records
     * - Calculate the merge fan-in and merge fan-out in records and the cluster size
     * - Create a run manager for HDD and SSD
     */
    void configure();

    /**
     * @brief Set the spillTo storage device for spilling when this storage device is full.
     */
    void setSpillTo(Storage *spillTo) {
        this->spillTo = spillTo;
        printv("%s set SpillTo to %s\n", this->getName().c_str(), spillTo->getName().c_str());
    }

    /**
     * @brief Destructor for the Storage class
     */
    ~Storage() {
        if (runManager != nullptr) {
            delete runManager;
            runManager = nullptr;
        }
    }

  public:
    // ------------------------ configurations ---------------------------------
    std::string getName() const { return name; }
    RowCount getCapacityInRecords() const { return CAPACITY_IN_BYTES / Config::RECORD_SIZE; }
    RowCount getPageSizeInRecords() const { return PAGE_SIZE_IN_RECORDS; }
    RowCount getMergeFanInRecords() const { return MERGE_FANIN_IN_RECORDS; }
    RowCount getMergeFanOutRecords() const { return MERGE_FANOUT_IN_RECORDS; }
    int getMaxMergeFanIn() const { return MAX_MERGE_FAN_IN; }
    int getMaxMergeFanOut() const { return MAX_MERGE_FAN_OUT; }
    PageCount getClusterSize() const { return CLUSTER_SIZE; }


    // ----------------------------- time calculations -------------------------
    double getAccessTimeInSec(RowCount nRecords) const {
        if (this->name == SSD_NAME) {
            Config::SSD_COUNT++;
        } else if (this->name == DISK_NAME) {
            Config::HDD_COUNT++;
        }
        return this->LATENCY + (nRecords * Config::RECORD_SIZE) / this->BANDWIDTH;
    }
    double getAccessTimeInMicro(RowCount nRecords) const {
        return (this->getAccessTimeInSec(nRecords) * 1000 * 1000);
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
    void fillupSpace(RowCount nRecords) {
        if (_filled + nRecords > getCapacityInRecords()) {
            throw std::runtime_error("ERROR: filling more records than capacity in " + this->name);
        }
        _filled += nRecords;
    }
    void freeSpace(RowCount nRecords) {
        if (nRecords > _filled) {
            std::string msg = "ERROR: freeing " + std::to_string(nRecords) +
                              " records than filled in " + std::to_string(_filled) + " in " +
                              this->name;
            throw std::runtime_error(msg);
        }
        _filled -= nRecords;
    }
    void fillInputCluster(RowCount nRecords) {
        if (_filledInputClusters + nRecords > _totalSpaceInInputClusters) {
            throw std::runtime_error("ERROR: filling more records than capacity in input clusters");
        }
        _filledInputClusters += nRecords;
    }
    void fillOutputCluster(RowCount nRecords) {
        if (_filledOutputClusters + nRecords > _totalSpaceInOutputClusters) {
            throw std::runtime_error(
                "ERROR: filling more records than capacity in output clusters");
        }
        _filledOutputClusters += nRecords;
    }
    void freeInputCluster(RowCount nRecords) {
        if (nRecords > _filledInputClusters) {
            throw std::runtime_error("ERROR: freeing more records than filled in input clusters");
        }
        _filledInputClusters -= nRecords;
    }
    void freeOutputCluster(RowCount nRecords) {
        if (nRecords > _filledOutputClusters) {
            throw std::runtime_error("ERROR: freeing more records than filled in output clusters");
        }
        _filledOutputClusters -= nRecords;
    }
    void resetAllFilledSpace() {
        _filled = 0;
        _filledInputClusters = 0;
        _filledOutputClusters = 0;
    }

    // --------------------------- merging states ------------------------------
    // merging states
    void resetMergeState() {
        _effectiveClusterSize = 0;
        _filledInputClusters = 0;
        _filledOutputClusters = 0;
        _totalSpaceInInputClusters = 0;
        _totalSpaceInOutputClusters = 0;
    }

    // --------------------------- FILE I/O ------------------------------------
    std::string getReadFilePath() const { return readFilePath; }
    bool readFrom(const std::string &filePath);
    std::streampos getReadPosition() { return readFile.tellg(); }
    RowCount readRecords(char *data, RowCount nRecords);
    // cleanup
    void closeRead();

    // ---------------------------- run management ----------------------------
    RunWriter *getRunWriter();
    RowCount writeNextChunk(RunWriter *writer, Run *run);
    void closeWriter(RunWriter *writer);
    void addRunFile(std::string filename, RowCount nRecords) {
        runManager->addRunFile(filename, nRecords);
    }
    std::string getBaseDir() {
        if (runManager == nullptr) { return ""; }
        return runManager->getBaseDir();
    }
    // ---- spill session ----
    void spill(RunWriter *writer);
    RunWriter *startSpillSession();
    void endSpillSession(RunWriter *writer, bool deleteCurrFile = false);
    int getRunfilesCount() {
        if (runManager == nullptr) { return 0; }
        return runManager->getStoredRunsSortedBySize().size();
    }
    std::string getRunfile(int index) {
        if (runManager == nullptr) { return ""; }
        return runManager->getStoredRunsSortedBySize()[index].first;
    }

    // ---------------------------- printing -----------------------------------
    std::string reprUsageDetails();
    void printStoredRunFiles();

}; // class Storage


#endif // _STORAGE_H_
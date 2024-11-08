#include "Storage.h"
#include "Losertree.h"
#include <algorithm>
#include <cmath>
#include <vector>


// =========================================================
// ------------------------ RunManager ---------------------
// =========================================================


RunManager::RunManager(std::string deviceName) {
    // Create a directory for the device runs
    baseDir = deviceName + "_runs";
    nextRunIndex = 0;
    runFiles.clear();

    struct stat st = {0};
    if (stat(baseDir.c_str(), &st) == -1) {

        // If the directory does not exist, create it
        mkdir(baseDir.c_str(), 0700);

    } else {

        // If the dir exits, delete all run files in the directory
        int counter = 0;
        DIR *dir = opendir(baseDir.c_str());
        if (dir) {
            struct dirent *entry;
            while ((entry = readdir(dir)) != nullptr) {
                struct stat path_stat;
                std::string filePath = baseDir + "/" + entry->d_name;
                stat(filePath.c_str(), &path_stat);
                if (S_ISREG(path_stat.st_mode)) {
                    std::remove(filePath.c_str());
                    counter++;
                }
            }
            closedir(dir);
        }
        if (counter > 0) {
            printvv("WARNING: Deleted %d run files in %s\n", counter, baseDir.c_str());
        }
    }
    printv("\tINFO: RunManager initialized for %s\n", deviceName.c_str());
}

RunManager::~RunManager() {
    // Give a warning if there are any run files left
    if (getRunInfoFromDir().size() > 0) {
        printvv("WARNING: %d run files left in %s\n", runFiles.size(), baseDir.c_str());
    }
    printv("\tINFO: RunManager Deleted for %s\n", baseDir.c_str());
}


std::string RunManager::getNextRunFileName() {
    struct stat st = {0};
    if (stat(baseDir.c_str(), &st) == -1) {
        mkdir(baseDir.c_str(), 0777);
    }
    std::string filename = baseDir + "/r" + std::to_string(nextRunIndex++) + ".txt";
    return filename;
}


std::vector<std::string> RunManager::getRunInfoFromDir() {
    std::vector<std::string> runFiles;
    DIR *dir = opendir(baseDir.c_str());
    if (dir) {
        struct dirent *entry;
        while ((entry = readdir(dir)) != nullptr) {
            struct stat path_stat;
            std::string filePath = baseDir + "/" + entry->d_name;
            stat(filePath.c_str(), &path_stat);
            if (S_ISREG(path_stat.st_mode)) {
                runFiles.push_back(entry->d_name);
            }
        }
        closedir(dir);
    }
    std::sort(runFiles.begin(), runFiles.end());
    return runFiles;
}

//
std::vector<std::pair<std::string, RowCount>> &RunManager::getStoredRunsSortedBySize() {
    std::sort(runFiles.begin(), runFiles.end(),
              [&](const std::pair<std::string, RowCount> &a,
                  const std::pair<std::string, RowCount> &b) { return a.second < b.second; });
    return runFiles;
}


// =========================================================
// -------------------------- Storage ----------------------
// =========================================================


Storage::Storage(std::string name, ByteCount capacity, int bandwidth, double latency)
    : name(name), CAPACITY_IN_BYTES(capacity), BANDWIDTH(bandwidth), LATENCY(latency) {

    printvv("Storage %s\n", name.c_str());
    if (CAPACITY_IN_BYTES == INFINITE_CAPACITY) {
        printvv("\tCapacity: Infinite\n");
    } else {
        printvv("\tCapacity %s\n", getSizeDetails(CAPACITY_IN_BYTES).c_str());
    }
    printvv("\tBandwidth %d MB/s, Latency %3.1lf ms\n", BYTE_TO_MB(BANDWIDTH), SEC_TO_MS(LATENCY));

    this->configure();
    if (this->name != DRAM_NAME) {
        this->runManager = new RunManager(this->name);
    }
    flushvv();
}


void Storage::configure() {
    // Calculate the page size in records
    ByteCount nBytes = this->BANDWIDTH * this->LATENCY;
    nBytes = RoundUp(nBytes, 4 * 1024); // round up to 4KB
    PAGE_SIZE_IN_RECORDS = nBytes / Config::RECORD_SIZE;
    PAGE_SIZE_IN_RECORDS = std::max((RowCount)1, PAGE_SIZE_IN_RECORDS);
    printvv("\tConfigured %s\n", this->name.c_str());
    printvv("\tPage %s\n",
            getSizeDetails(this->PAGE_SIZE_IN_RECORDS * Config::RECORD_SIZE).c_str());

    if (this->name != DISK_NAME) {

        // Calculate a rough cluster size (buffer size per run) in pages
        RowCount nRecords = CAPACITY_IN_BYTES / Config::RECORD_SIZE;
        PageCount nPages = nRecords / PAGE_SIZE_IN_RECORDS;
        CLUSTER_SIZE = nPages / (MAX_MERGE_FAN_IN + MAX_MERGE_FAN_OUT);

        // Calculate the merge fan-in and merge fan-out
        MERGE_FANIN_IN_RECORDS = MAX_MERGE_FAN_IN * CLUSTER_SIZE * PAGE_SIZE_IN_RECORDS;
        MERGE_FANOUT_IN_RECORDS = getCapacityInRecords() - MERGE_FANIN_IN_RECORDS;

        // Print the configurations
        printvv("\tCluster Size: %d pages / %s\n", CLUSTER_SIZE,
                getSizeDetails(CLUSTER_SIZE * PAGE_SIZE_IN_RECORDS * Config::RECORD_SIZE).c_str());
        printvv("\tInput Buffer Total Size: %llu records\n", this->MERGE_FANIN_IN_RECORDS);
        printvv("\tOutput Buffer Total Size: %llu records\n", this->MERGE_FANOUT_IN_RECORDS);
    }
}


// ------------------------------- Run Management ------------------------------


RunWriter *Storage::getRunWriter() {
    if (runManager == nullptr) {
        throw std::runtime_error("ERROR: RunManager is not initialized");
    }
    std::string filename = runManager->getNextRunFileName();
    return new RunWriter(filename);
}


void Storage::spill(RunWriter *writer) {
    if (this->spillTo == nullptr) {
        printvv("ERROR: %s is full, no spillTo device\n", this->name.c_str());
        throw std::runtime_error("Error: Storage is full, no spillTo device");
    }

    // Start a spill session if not already started
    if (spillWriter == nullptr) {
        spillWriter = startSpillSession();
    }

    // Close the current writer file
    writer->close();

    // Copy the current writer file content to spillWriter
    RowCount nRecord = spillWriter->writeFromFile(writer->getFilename(), writer->getCurrSize());
    if (nRecord != writer->getCurrSize()) {
        printvv("ERROR: Failed to copy %lld records to %s\n", writer->getCurrSize(),
                spillWriter->getFilename().c_str());
        assert(nRecord == writer->getCurrSize() && "Failed to copy all records to spillWriter");
    }
    spillTo->fillupSpace(nRecord);
    // reset the filesize to 0
    writer->reset();
    // update the storage usage (free up the space in this storage)
    this->freeSpace(nRecord);
    printss("\t\tSTATE -> %s is full, Spill to %s %lld records\n", this->name.c_str(),
            spillTo->name.c_str(), nRecord);
    printss("\t\tACCESS -> A write to %s was made with size %llu bytes and latency %.2lf us\n",
            spillTo->getName().c_str(), nRecord * Config::RECORD_SIZE,
            spillTo->getAccessTimeInMicro(nRecord));
    flushvv();
}


RowCount Storage::writeNextChunk(RunWriter *writer, Run *run) {
    RowCount _empty = this->getTotalEmptySpaceInRecords();
    if (run->getSize() > _empty) {
        spill(writer);
    }
    /** assumption: spilling the current file to disk will free up enough space */
    RowCount nRecord = writer->writeNextRun(run);
    _filled += nRecord;
    return nRecord;
}

void Storage::closeWriter(RunWriter *writer) {
    if (this->runManager == nullptr) {
        printv("ERROR: RunManager is null in %s\n", this->name.c_str());
        return;
    }
    if (writer == nullptr) {
        printv("ERROR: Writer is null in %s\n", this->name.c_str());
        return;
    }
    RowCount nRecords = writer->getCurrSize();
    runManager->addRunFile(writer->getFilename(), nRecords);

    if (spillWriter != nullptr) {
        endSpillSession(writer, true);
    } else {
        writer->close();
        delete writer;
    }
}


RunWriter *Storage::startSpillSession() {
    spillWriter = spillTo->getRunWriter();
    printv("\t\t\tINFO: Spill writer created for %s\n", this->name.c_str());
    return spillWriter;
}

void Storage::endSpillSession(RunWriter *currDeviceWriter, bool deleteCurrFile) {
    if (spillWriter != nullptr) {
        if (currDeviceWriter != nullptr) {
            printvv("\t\tSpill to %s leftovers\n", this->name.c_str(), spillTo->name.c_str());
            spill(currDeviceWriter);
            if (deleteCurrFile && (!currDeviceWriter->isDeletedFile())) {
                this->freeSpace(currDeviceWriter->getCurrSize());
                printv("\t\t\tfreeing up %lld bytes\n", currDeviceWriter->getCurrSize());
                flushv();
                currDeviceWriter->close();
                currDeviceWriter->deleteFile();
                this->runManager->removeRunFile(currDeviceWriter->getFilename());
            } else {
                currDeviceWriter->close();
            }
        }

        RowCount nRecords = spillWriter->getCurrSize();
        spillTo->addRunFile(spillWriter->getFilename(), nRecords);
        spillWriter->close();
        delete spillWriter;
        spillWriter = nullptr;
        printv("\t\t\tINFO: Spill writer wrote %lld records. END\n", nRecords);
    }
}

// ------------------------------- Merging state -------------------------------


// --------------------------------- File IO -----------------------------------


bool Storage::readFrom(const std::string &filePath) {
    if (readFile.is_open())
        readFile.close();
    readFilePath = filePath;
    readFile.open(readFilePath, std::ios::binary);
    if (!readFile.is_open()) {
        printvv("ERROR: Failed to open read file '%s'\n", readFilePath.c_str());
        return false;
    }
    readFile.seekg(0, std::ios::beg);
    if (!readFile) {
        printv("ERROR: Failed to seek to the beginning of '%s'\n", readFilePath.c_str());
        return false;
    }
    // printvv("DEBUG: Opened readFile '%s', curr pos %llu\n", readFilePath.c_str(),
    // readFile.tellg());
    return true;
}

RowCount Storage::readRecords(char *data, RowCount nRecords) {
    if (!readFile.is_open()) {
        printvv("ERROR: Read file '%s' is not open\n", readFilePath.c_str());
        return 0;
    }
    readFile.read(data, nRecords * Config::RECORD_SIZE);
    ByteCount nBytes = readFile.gcount();
    return nBytes / Config::RECORD_SIZE;
}

void Storage::closeRead() {
    if (readFile.is_open())
        readFile.close();
}


// ---------------------------- printing -----------------------------------
std::string Storage::reprUsageDetails() {
    std::string state = "\n\t\t\t" + this->name + " usage details: ";
    state += "\n\t\t\t\tcapacity: " + (CAPACITY_IN_BYTES == INFINITE_CAPACITY
                                           ? "Infinite"
                                           : std::to_string(getCapacityInRecords()) + " records");
    state += "\n\t\t\t\t_filled (with runfiles): " + std::to_string(_filled) + " records";
    if (this->name != DISK_NAME) {
        state += "\n\t\t\t\tinputcluster: " + std::to_string(_filledInputClusters) + " out of " +
                 std::to_string(_totalSpaceInInputClusters) + " records";
        state += ", \n\t\t\t\toutputcluster: " + std::to_string(_filledOutputClusters) +
                 " out of " + std::to_string(_totalSpaceInOutputClusters) + " records";
        state += ", \n\t\t\t\tcluster size (inbuf size per run): " +
                 std::to_string(_effectiveClusterSize) + " records";
    }
    state += "\n\t\t\t\ttotal filled: " + std::to_string(getTotalFilledSpaceInRecords()) +
             " out of " + std::to_string(getCapacityInRecords()) + " records";
    state += ", total empty space: " + std::to_string(getTotalEmptySpaceInRecords()) + " records";
    if (runManager != nullptr) {
        state += "\n\t\t\t" + runManager->repr();
    }
    return state;
}

void Storage::printStoredRunFiles() {
    if (runManager == nullptr) {
        printv("ERROR: RunManager is not initialized in %s\n", this->name.c_str());
        return;
    }
    std::vector<std::pair<std::string, RowCount>> runFiles =
        runManager->getStoredRunsSortedBySize();
    printv("\t\t\t%s has %d run files, totalRecords %lld\n", this->name.c_str(), runFiles.size(),
           runManager->getTotalRecords());
    for (auto &run : runFiles) {
        printv("\t\t\t\t%s: %s\n", run.first.c_str(),
               getSizeDetails(run.second * Config::RECORD_SIZE).c_str());
    }
}

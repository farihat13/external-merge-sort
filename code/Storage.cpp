#include "Storage.h"
#include "Losertree.h"
#include <algorithm>
#include <vector>


// =========================================================
// ------------------------ RunManager ---------------------
// =========================================================


RunManager::RunManager(std::string deviceName) {
    baseDir = deviceName + "_runs";
    nextRunIndex = 0;
    runFiles.clear();

    struct stat st = {0};
    if (stat(baseDir.c_str(), &st) == -1) {
        // if the directory does not exist, create it
        mkdir(baseDir.c_str(), 0700);
    } else {
        // if the dir exits, delete all run files in the directory
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
    // give a warning if there are any run files left
    if (getRunInfoFromDir().size() > 0) {
        printvv("WARNING: %d run files left in %s\n", runFiles.size(), baseDir.c_str());
    }
}


std::string RunManager::getNextRunFileName() {
    struct stat st = {0};
    if (stat(baseDir.c_str(), &st) == -1) {
        mkdir(baseDir.c_str(), 0777);
    }
    std::string filename = baseDir + "/r" + std::to_string(nextRunIndex++) + ".txt";
    return filename;
}


// validatations
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

    printv("INFO: Storage %s\n", name.c_str());
    if (CAPACITY_IN_BYTES == INFINITE_CAPACITY) {
        printv("\tCapacity: Infinite\n");
    } else {
        printv("\tCapacity %s\n", getSizeDetails(CAPACITY_IN_BYTES).c_str());
    }
    printv("\tBandwidth %d MB/s, Latency %3.1lf ms\n", BYTE_TO_MB(BANDWIDTH), SEC_TO_MS(LATENCY));

    this->configure();
    this->runManager = new RunManager(this->name);
}

void Storage::configure() {
    // TODO: configure MERGE_FAN_IN and MERGE_FAN_OUT

    // calculate the page size in records
    int nBytes = this->BANDWIDTH * this->LATENCY;
    nBytes = ROUNDUP_4K(nBytes);
    this->PAGE_SIZE_IN_RECORDS = std::ceil(nBytes / Config::RECORD_SIZE);

    // calculate the cluster size in pages
    int nRecords = this->CAPACITY_IN_BYTES / Config::RECORD_SIZE;
    int nPages = nRecords / this->PAGE_SIZE_IN_RECORDS;
    this->CLUSTER_SIZE = nPages / (this->MERGE_FAN_IN + this->MERGE_FAN_OUT);

    // calculate the merge fan-in and merge fan-out
    this->MERGE_FANIN_IN_RECORDS =
        this->MERGE_FAN_IN * this->CLUSTER_SIZE * this->PAGE_SIZE_IN_RECORDS;
    this->MERGE_FANOUT_IN_RECORDS = getCapacityInRecords() - this->MERGE_FANIN_IN_RECORDS;

    // print the configurations
    printv("\tINFO: Configured %s\n", this->name.c_str());
    printv("\t\tPage %s\n",
           getSizeDetails(this->PAGE_SIZE_IN_RECORDS * Config::RECORD_SIZE).c_str());
    printv("\t\tCluster Size: %d pages / %s\n", this->CLUSTER_SIZE,
           getSizeDetails(this->CLUSTER_SIZE * this->PAGE_SIZE_IN_RECORDS * Config::RECORD_SIZE)
               .c_str());
    printv("\t\t_mergeFanIn: %llu records\n", this->MERGE_FANIN_IN_RECORDS);
    printv("\t\t_mergeFanOut: %llu records\n", this->MERGE_FANOUT_IN_RECORDS);
}

// ------------------------------- Run Management ------------------------------

RunWriter *Storage::getRunWriter() {
    std::string filename = runManager->getNextRunFileName();
    return new RunWriter(filename);
}

void Storage::closeWriter(RunWriter *writer) {
    // this->addRunFile(writer->getFilename(), writer->getSize());
    RowCount nRecords = writer->getCurrSize();
    runManager->addRunFile(writer->getFilename(), nRecords);
    _filled += nRecords;
    writer->close();
    delete writer;
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

bool Storage::writeTo(const std::string &filePath) {
    if (writeFile.is_open())
        writeFile.close();
    writeFilePath = filePath;
    writeFile.open(writeFilePath, std::ios::binary);
    if (!writeFile.is_open()) {
        printvv("ERROR: Failed to open write file '%s'\n", writeFilePath.c_str());
        return false;
    }
    writeFile.seekp(0, std::ios::beg);
    if (!writeFile) {
        printv("ERROR: Failed to seek to the beginning of '%s'\n", writeFilePath.c_str());
        return false;
    }
    assert(writeFile.is_open() && "Failed to open write file");
    // printvv("DEBUG: Opened write file '%s', curr pos %llu\n", writeFilePath.c_str(),
    //         writeFile.tellp());
    return true;
}

char *Storage::readRecords(RowCount *toRead) {
    int nRecords = *toRead;
    if (!readFile.is_open()) {
        printvv("ERROR: Read file '%s' is not open\n", readFilePath.c_str());
        return nullptr;
    }

    char *data = new char[nRecords * Config::RECORD_SIZE];
    readFile.read(data, nRecords * Config::RECORD_SIZE);
    int nBytes = readFile.gcount();
    // printv("\t\tRead %d bytes from '%s', filepos %llu\n", nBytes, readFilePath.c_str(),
    //        readFile.tellg());
    *toRead = nBytes / Config::RECORD_SIZE;
    return data;
}

void Storage::closeRead() {
    if (readFile.is_open())
        readFile.close();
}

void Storage::closeWrite() {
    if (writeFile.is_open())
        writeFile.close();
}


// ---------------------------- printing -----------------------------------
std::string Storage::reprUsageDetails() {
    std::string state = "\n\t" + this->name + " usage details: ";
    state += "\n\t\t_filled: " + std::to_string(_filled) + " records";
    state += "\n\t\tinputcluster: " + std::to_string(_filledInputClusters) + " out of " +
             std::to_string(_totalSpaceInInputClusters) + " records, ";
    state += "\n\t\toutputcluster: " + std::to_string(_filledOutputClusters) + " out of " +
             std::to_string(_totalSpaceInOutputClusters) + " records";
    state += "\n\t\teffective cluster size: " + std::to_string(_effectiveClusterSize) + " records";
    state += "\n\t\ttotal filled: " + std::to_string(getTotalFilledSpaceInRecords()) + " records";
    state += "\n\t\ttotal capacity: " + std::to_string(getCapacityInRecords()) + " records";
    state +=
        "\n\t\ttotal empty space: " + std::to_string(getTotalEmptySpaceInRecords()) + " records";
    state += "\n\t\t" + runManager->repr();
    return state;
}


// =========================================================
// ----------------------- RunStreamer ---------------------
// =========================================================


RunStreamer::RunStreamer(Run *run) : currentRecord(run->getHead()), reader(nullptr) {
    if (currentRecord == nullptr) {
        throw std::runtime_error("Error: RunStreamer initialized with empty run");
    }
}

RunStreamer::RunStreamer(RunReader *reader, Storage *device, int readAhead)
    : reader(reader), device(device), readAhead(readAhead) {

    RowCount nRecords = 0;
    currentPage = reader->readNextPage();
    nRecords += currentPage->getSizeInRecords();
    currentRecord = currentPage->getFirstRecord();
    if (currentRecord == nullptr) {
        throw std::runtime_error("Error: RunStreamer initialized with empty run");
    }
    if (readAhead > 1) {
        RowCount n = readAheadPages(readAhead - 1);
        nRecords += n;
    }
    printvv("DEBUG: constructor: readAhead %d pages, %ld records\n", readAhead, nRecords);
    printvv("STATE -> Read %lld records from %s\n", nRecords, reader->getFilename().c_str());
    printvv("ACCESS -> A read from %s was made with size %llu bytes and latency %d ms\n",
            device->getName().c_str(), nRecords * Config::RECORD_SIZE,
            device->getAccessTimeInMillis(nRecords));
}

RowCount RunStreamer::readAheadPages(int nPages) {
    // printvv("DEBUG: readAheadPages(%d)\n", nPages);
    RowCount nRecords = 0;
    Page *page = currentPage;
    for (int i = 0; i < nPages; i++) {
        Page *p = reader->readNextPage();
        if (p == nullptr) {
            break;
        }
        nRecords += p->getSizeInRecords();
        page->addNextPage(p);
        page = p;
    }
    return nRecords;
}

Record *RunStreamer::moveNext() {
    // if reader does not exist
    if (reader == nullptr) {
        if (currentRecord->next == nullptr) {
            return nullptr;
        }
        currentRecord = currentRecord->next;
        return currentRecord;
    }

    // if reader exists
    if (currentRecord == currentPage->getLastRecord()) {
        // if this is the last record in the page, move to the next page
        currentPage = currentPage->getNext(); // move to the next page
        if (currentPage != nullptr) {
            currentRecord = currentPage->getFirstRecord(); // set current record to first record
        } else {
            // if no more page in memory, read next `readAhead` pages
            RowCount nRecords = 0;
            currentPage = reader->readNextPage();
            if (currentPage == nullptr) {
                return nullptr;
            }
            nRecords += currentPage->getSizeInRecords();
            currentRecord = currentPage->getFirstRecord();
            if (readAhead > 1) {
                RowCount n = readAheadPages(readAhead - 1);
                nRecords += n;
            }
            printvv("DEBUG: readAhead %d pages, %ld records\n", readAhead, nRecords);
            printvv("STATE -> Read %lld records from %s\n", nRecords,
                    reader->getFilename().c_str());
            printvv("ACCESS -> A read from %s was made with size %llu bytes and latency %d ms\n",
                    device->getName().c_str(), nRecords * Config::RECORD_SIZE,
                    device->getAccessTimeInMillis(nRecords));
        }
    } else {
        // if this is not the last record in the page,
        // move to the next record
        currentRecord = currentRecord->next;
    }
    return currentRecord;
}

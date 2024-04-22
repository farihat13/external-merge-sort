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
    if (CAPACITY_IN_BYTES == INT_MAX) {
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


// =========================================================
// -------------------------- Disk -------------------------
// =========================================================


HDD *HDD::instance = nullptr;


HDD::HDD(std::string name, ByteCount capacity, int bandwidth, double latency)
    : Storage(name, capacity, bandwidth, latency) {}


// =========================================================
// -------------------------- SSD -------------------------
// =========================================================

SSD *SSD::instance = nullptr;

SSD::SSD() : HDD("SSD", Config::SSD_CAPACITY, Config::SSD_BANDWIDTH, Config::SSD_LATENCY) {}


void getConstants(SSD *ssd, RowCount &ssdPageSize, RowCount &hddPageSize, RowCount &ssdCapacity,
                  RowCount &ssdEmptySpace, RowCount &dramCapacity) {
    ssdPageSize = ssd->getPageSizeInRecords();
    hddPageSize = HDD::getInstance()->getPageSizeInRecords();
    ssdCapacity = ssd->getCapacityInRecords();
    ssdEmptySpace = ssd->getTotalEmptySpaceInRecords();
    dramCapacity = DRAM::getInstance()->getCapacityInRecords();
    printv("\t\tdramEmptySpace: %lld records\n", DRAM::getInstance()->getCapacityInRecords());
    printv("\t\tssdPageSize: %lld records\n", ssdPageSize);
    printv("\t\thddPageSize: %lld records\n", hddPageSize);
    printv("\t\tssdCapacity: %lld records\n", ssdCapacity);
    printv("\t\tssdEmptySpace: %lld records, %lld pages\n", ssdEmptySpace,
           ssdEmptySpace / ssdPageSize);
}


void SSD::spillRunsToHDD(HDD *hdd) {
    TRACE(true);

    Storage *_dram = DRAM::getInstance();
    Storage *_ssd = this; // SSD::getInstance();
    Storage *_hdd = HDD::getInstance();

    // print all device information
    printvv("DEBUG: before spill from SSD to HDD: %s\n", _dram->reprUsageDetails().c_str());
    printv("%s\n", _ssd->reprUsageDetails().c_str());
    printv("%s\n", _hdd->reprUsageDetails().c_str());

    // validate the total runsize does not exceed merge fan-in
    assert(this->runManager->getTotalRecords() == this->_filled);
    if (this->_filled > getMergeFanInRecords()) {
        printvv("ERROR: Run size %lld exceeds merge fan-in %lld\n", this->_filled,
                getMergeFanInRecords());
        throw std::runtime_error("Run size exceeds merge fan-in in " + this->getName());
    }

    RowCount ssdPageSize, hddPageSize, ssdCapacity, ssdEmptySpace, dramCapacity;
    getConstants(this, ssdPageSize, hddPageSize, ssdCapacity, ssdEmptySpace, dramCapacity);
    // get the run files sorted by size
    std::vector<std::pair<std::string, RowCount>> runFiles =
        runManager->getStoredRunsSortedBySize();
    printv("\t\t#runFiles: %d\n", runFiles.size());
    printv("\t\tstored runsize in %s: %lld records, %lld pages\n", this->getName().c_str(),
           this->_filled, this->_filled / ssdPageSize);


    RowCount _mergeFanOutDRAM = DRAM::getInstance()->getMergeFanOut() * ssdPageSize;
    RowCount _mergeFanInDRAM = ROUNDDOWN(dramCapacity - _mergeFanOutDRAM, ssdPageSize);
    printv("\t\tmergeFanInDRAM: %lld records, %lld ssdPages\n", _mergeFanInDRAM,
           _mergeFanInDRAM / ssdPageSize);
    printv("\t\tmergeFanOutDRAM: %lld records, %lld ssdPages\n", _mergeFanOutDRAM, 2);

    if (runFiles.size() > _mergeFanInDRAM) {
        printvv("ERROR: More runs than mergeFanInDRAM\n");
        throw std::runtime_error("More runs than mergeFanInDRAM");
    }

    int inputBufSizePerRun = _mergeFanInDRAM / (runFiles.size() * ssdPageSize);
    // adjust mergeFanInDRAM and mergeFanOutDRAM
    _mergeFanInDRAM = inputBufSizePerRun * runFiles.size() * ssdPageSize;
    _mergeFanOutDRAM = ROUNDDOWN(dramCapacity - _mergeFanInDRAM, ssdPageSize);
    printv("\t\tadjusted mergeFanInDRAM: %lld records, %lld ssdPages\n", _mergeFanInDRAM,
           _mergeFanInDRAM / ssdPageSize);
    printv("\t\tadjusted mergeFanOutDRAM: %lld records, %lld ssdPages\n", _mergeFanOutDRAM, 2);
    printv("\t\treadAhead: %d\n", inputBufSizePerRun);

    // 1. read the runs from SSD
    std::vector<RunStreamer *> runStreamers;
    RowCount willFill = 0;
    for (int i = 0; i < runFiles.size(); i++) {
        std::string runFilename = runFiles[i].first;
        RowCount runSize = runFiles[i].second;
        printv("Run %d: %s, %lld records\n", i, runFilename.c_str(), runSize);
        RunReader *reader = new RunReader(runFilename, runSize, ssdPageSize);
        RowCount willRead = std::min((inputBufSizePerRun * ssdPageSize), runSize);
        willFill += willRead;
        RunStreamer *runStreamer = new RunStreamer(reader, this, inputBufSizePerRun);
        runStreamers.push_back(runStreamer);
    }
    printv("\t\twillFill %llu\n", willFill);
    flushv();

    // 2. remaining runs fit in DRAM, merge them
    LoserTree loserTree;
    loserTree.constructTree(runStreamers);
    printv("DEBUG: Merging %d runs in DRAM\n", runStreamers.size());
    Record *head = new Record();
    Record *current = head;
    RowCount nSorted = 0;
    RunWriter writer("merged.txt");
    while (true) {
        Record *winner = loserTree.getNext();
        if (winner == NULL) {
            break;
        }
        flushv();
        current->next = winner;
        current = current->next;
        nSorted++;

        if (nSorted * Config::RECORD_SIZE >= _mergeFanOutDRAM) {
            // write the sorted records to SSD
            Run merged(head->next, ssdPageSize);
            this->storeRun(merged);
            printv("DEBUG: Merged %lld records in DRAM\n", nSorted);
            // reset the head
            head = new Record();
            current = head;
        }
    }
    Record *merged = head->next;
    Run mergedRun(merged, nSorted);
    printv("DEBUG: Merged %lld records in DRAM\n", nSorted);

    exit(0);

    // merge the runs in SSD and store the merged run in HDD
}


// =========================================================
// -------------------- In-memory Quick Sort ---------------
// =========================================================

/**
 * @brief swap two records
 */
void swap(Record &a, Record &b) {
    // Record temp = a;
    // a = b;
    // b = temp;
    std::swap_ranges(a.data, a.data + Config::RECORD_SIZE, b.data);
}

/**
 * @brief partition the array using the last element as pivot
 */
int partition(std::vector<Record *> &records, int low, int high) {
    Record *pivot = records[high]; // choosing the last element as pivot
    int i = (low - 1);             // Index of smaller element

    for (int j = low; j <= high - 1; j++) {
        // If current element is smaller than the pivot, increment index of
        // smaller element and swap the elements
        if (*records[j] < *pivot) {
            i++;
            std::swap(records[i], records[j]);
            // swap(records[i], records[j]);
        }
    }
    // swap(records[i + 1], records[high]);
    std::swap(records[i + 1], records[high]);
    return (i + 1);
}

void quickSortRecursive(std::vector<Record *> &records, int low, int high) {
    if (low < high) {
        int pi = partition(records, low, high);
        quickSortRecursive(records, low, pi - 1);
        quickSortRecursive(records, pi + 1, high);
    }
}

void quickSort(std::vector<Record *> &records) {
    quickSortRecursive(records, 0, records.size() - 1);
}

// =========================================================
// -------------------------- DRAM -------------------------
// =========================================================


DRAM *DRAM::instance = nullptr;

DRAM::DRAM()
    : Storage("DRAM", Config::DRAM_CAPACITY, Config::DRAM_BANDWIDTH, Config::DRAM_LATENCY) {
    this->reset();
}


void DRAM::loadRecordsToDRAM(char *data, RowCount nRecords) {
    // create a linked list of records
    Record *tail = nullptr;
    for (int i = 0; i < nRecords; i++) {
        Record *rec = new Record(data);
        if (_head == NULL) {
            _head = rec;
            tail = rec;
        } else {
            tail->next = rec;
            tail = rec;
        }
        // update
        data += Config::RECORD_SIZE;
    }
    _filled += nRecords;
    // printvv("DEBUG: Loaded %lld records to DRAM\n", nRecords);
}


void DRAM::genMiniRuns(RowCount nRecords) {
    TRACE(true);
#if defined(_VALIDATE)
    // validate the head contains nRecords and does not exceed the DRAM capacity
    RowCount count = 0;
    Record *rec = _head;
    while (rec != nullptr) {
        count++;
        rec = rec->next;
    }
    printv("VALIDATE: %lld out of %lld records loaded in RAM\n", count, nRecords);
    printv("\t\tDRAM filled %llu out of %llu Records\n", count,
           Config::DRAM_CAPACITY / Config::RECORD_SIZE);
    flushv();
    assert(count == nRecords && "VALIDATE: Invalid number of records loaded in RAM");
    assert(count <= Config::DRAM_CAPACITY / Config::RECORD_SIZE &&
           "VALIDATE: Number of records exceeds DRAM capacity");
#endif

    // sort the records in cache
    RowCount _cacheSize = Config::CACHE_SIZE / Config::RECORD_SIZE;
    assert(_miniruns.size() == 0 && "ERROR: miniruns is not empty");
    // std::vector<int> runSizes;
    Record *curr = _head;
    for (int i = 0; i < nRecords; i += _cacheSize) {
        std::vector<Record *> records;
        for (int j = 0; j < _cacheSize && curr != nullptr; j++) {
            records.push_back(curr);
            curr = curr->next;
        }
        quickSort(records);
        // update the next pointer
        for (int j = 0; j < records.size() - 1; j++) {
            records[j]->next = records[j + 1];
        }
        records.back()->next = nullptr;
        // create a run
        Run run(records[0], records.size());
        _miniruns.push_back(run);
    }
    printv("DEBUG: Sorted %lld records and generated %d runs in DRAM\n", nRecords,
           _miniruns.size());
}

void DRAM::mergeMiniRuns(HDD *outputStorage) {
    TRACE(true);
#if defined(_VALIDATE)
    // validate each run is sorted and the size of the run
    for (int i = 0; i < _miniruns.size(); i++) {
        Record *rec = _miniruns[i].getHead();
        int count = 1;
        // printv("Run %d: %d records\n", i, _miniruns[i].getSize());
        // printv("\t\t%s\n", rec->reprKey());
        while (rec->next != nullptr) {
            // printv("\t\t%s\n", rec->next->reprKey());
            assert(*rec < *rec->next && "VALIDATE: Run is not sorted");
            rec = rec->next;
            count++;
        }
        // printvv("VALIDATE: Run %d: %d records out of %d records\n", i, count,
        //         _miniruns[i].getSize());
        assert(count == _miniruns[i].getSize() && "VALIDATE: Run size mismatch");
    }
#endif

    // 1. spill the runs that don't fit in _mergeFanInRec to SSD
    printv("DEBUG: _mergeFanIn: %lld records, _mergeFanOut: %lld records\n", getMergeFanInRecords(),
           getMergeFanOutRecords());
    printv("before spill: %s\n", this->reprUsageDetails().c_str());
    printv("%s\n", outputStorage->reprUsageDetails().c_str());
    RowCount keepNRecords = 0;
    int i = 0;
    for (; i < _miniruns.size(); i++) {
        int size = _miniruns[i].getSize();
        if (keepNRecords + size < getMergeFanInRecords()) {
            keepNRecords += size;
        } else {
            break;
        }
    }
    if (i < _miniruns.size()) {
        printv("DEBUG: spill %d runs out of %d to SSDs\n", _miniruns.size() - i, _miniruns.size());
        printv("\t\tspill from %dth runs to SSD\n", i);
        int spillNRecords = 0;
        int j;
        for (j = i; j < _miniruns.size(); j++) {
            spillNRecords += _miniruns[j].getSize();
            outputStorage->storeRun(_miniruns[j]);
            this->freeMore(_miniruns[j].getSize());
            printv("\t\tSpilled run %d to SSD\n", j);
            flushv();
        }
        _miniruns.erase(_miniruns.begin() + i, _miniruns.end());

        printv("DEBUG: STATE -> %d runs spilled to SSD\n", j - i);
        printv(
            "ACCESS -> A write from RAM to SSD was made with size %llu bytes and latency %d ms\n",
            spillNRecords * Config::RECORD_SIZE, getAccessTimeInMillis(keepNRecords));
    } else {
        printv("DEBUG: All runs fit in DRAM\n");
    }
    printv("after spill: %s\n", this->reprUsageDetails().c_str());
    printv("%s\n", outputStorage->reprUsageDetails().c_str());


    // 2. remaining runs fit in DRAM, merge them
    LoserTree loserTree;
    loserTree.constructTree(_miniruns);

    printv("DEBUG: Merging %d runs in DRAM\n", _miniruns.size());
    Record *head = new Record();
    Record *current = head;
    RowCount nSorted = 0;
    while (true) {
        Record *winner = loserTree.getNext();
        if (winner == NULL) {
            break;
        }
        // printv("\t\tWinner: %s\n", winner->reprKey());
        flushv();
        current->next = winner;
        current = current->next;
        nSorted++;
        if (nSorted > keepNRecords) {
            printvv("ERROR: Merged run size exceeds %lld\n", keepNRecords);
            throw std::runtime_error("Merged run size exceeds");
        }
    }
    Record *merged = head->next;
    Run mergedRun(merged, keepNRecords);


#if defined(_VALIDATE)
    if (nSorted != keepNRecords) {
        printvv("ERROR: Merged run has %lld records, expected %lld\n", nSorted, keepNRecords);
    }
    assert(nSorted == keepNRecords && "VALIDATE: Merged run size mismatch");
    // validate the merged run
    Record *rec = merged;
    while (rec->next != nullptr) {
        if (!(*rec < *rec->next)) {
            printvv("ERROR: Merged run is not sorted\n");
            printv("\t\t%s vs. %s\n", rec->reprKey(), rec->next->reprKey());
        }
        assert(*rec < *rec->next && "VALIDATE: Merged run is not sorted");
        rec = rec->next;
    }
#endif
    printvv("DEBUG: Merged %d runs / %lld records\n", _miniruns.size(), keepNRecords);

    // 3. store the merged run in SSD
    outputStorage->storeRun(mergedRun);
    printvv("DEBUG: Stored merged run in SSD\n");
    printvv("ACCESS -> A write from RAM to SSD was made with size %llu bytes and latency %d ms\n",
            keepNRecords * Config::RECORD_SIZE, getAccessTimeInMillis(keepNRecords));

    // 4. reset the DRAM
    reset();
}
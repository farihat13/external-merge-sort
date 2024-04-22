
#include "StorageTypes.h"
#include "Losertree.h"


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
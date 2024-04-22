
#include "StorageTypes.h"
#include "Losertree.h"


// =========================================================
// -------------------------- Disk -------------------------
// =========================================================


HDD *HDD::instance = nullptr;


HDD::HDD(std::string name, ByteCount capacity, int bandwidth, double latency)
    : Storage(name, capacity, bandwidth, latency) {}

RowCount HDD::storeRun(Run &run) {

    RunWriter *writer = getRunWriter();
    RowCount nRecords = writer->writeNextRun(run);
    closeWriter(writer);
    return nRecords;
}


// RowCount storeRun(Run &run) {
//     if (getTotalEmptySpaceInRecords() < run.getSize()) {
//         printvv("ERROR: Run size %lld exceeds storage empty space\n", run.getSize());
//         printvv("%s\n", reprUsageDetails().c_str());
//         throw std::runtime_error(this->getName() + " is full, cannot store run");
//     }
//     RunWriter *writer = getRunWriter();
//     RowCount nRecords = writer->writeNextRun(run);
//     if (nRecords != run.getSize()) {
//         printvv("ERROR: Writing %lld records, expected %lld\n", nRecords, run.getSize());
//         throw std::runtime_error("Error: Writing run to file");
//     }
//     runManager->addRunFile(writer->getFilename(), nRecords);
//     _filled += nRecords;
//     // this->storeMore(nRecords);
//     writer->close();
//     delete writer;
//     return nRecords;
// }


// =========================================================
// -------------------------- SSD -------------------------
// =========================================================

SSD *SSD::instance = nullptr;

SSD::SSD() : HDD(SSD_NAME, Config::SSD_CAPACITY, Config::SSD_BANDWIDTH, Config::SSD_LATENCY) {}


/**
 * @brief helper function to get the constants for spillRunsToHDD
 */
void getConstants(SSD *ssd, RowCount &ssdPageSize, RowCount &hddPageSize, RowCount &ssdCapacity,
                  RowCount &ssdEmptySpace, RowCount &dramCapacity) {
    ssdPageSize = ssd->getPageSizeInRecords();
    hddPageSize = HDD::getInstance()->getPageSizeInRecords();
    ssdCapacity = ssd->getCapacityInRecords();
    ssdEmptySpace = ssd->getTotalEmptySpaceInRecords();
    dramCapacity = DRAM::getInstance()->getCapacityInRecords();
}


void SSD::spillRunsToHDD(HDD *hdd) {
    TRACE(true);

    Storage *_dram = DRAM::getInstance();
    Storage *_ssd = this; // SSD::getInstance();
    Storage *_hdd = HDD::getInstance();

    // print all device information
    printvv("DEBUG: before spill from SSD to HDD: %s\n", _dram->reprUsageDetails().c_str());
    printv("%s\n", _ssd->reprUsageDetails().c_str());
    printv("%s\n\n", _hdd->reprUsageDetails().c_str());
    // validate the total runsize does not exceed merge fan-in
    assert(this->runManager->getTotalRecords() == this->_filled);
    if (this->_filled > getMergeFanInRecords()) {
        printvv("ERROR: Run size %lld exceeds merge fan-in %lld\n", this->_filled,
                getMergeFanInRecords());
        throw std::runtime_error("Run size exceeds merge fan-in in " + this->getName());
    }

    RowCount ssdPageSize, hddPageSize, ssdCapacity, ssdEmptySpace, dramCapacity;
    getConstants(this, ssdPageSize, hddPageSize, ssdCapacity, ssdEmptySpace, dramCapacity);
    // get the runfile names sorted by size ascending
    std::vector<std::pair<std::string, RowCount>> runFiles =
        runManager->getStoredRunsSortedBySize();
    printv("\t\t%s: %d runfiles, %lld records, %lld pages\n", this->getName().c_str(),
           runFiles.size(), this->_filled, this->_filled / ssdPageSize);

    /**
     * 1. setup merging state
     */
    int fanIn = runFiles.size();
    _dram->setupMergeState(ssdPageSize, fanIn);
    printv("After setting up merging state: %s\n", _dram->reprUsageDetails().c_str());
    RowCount inBufSizePerRunDram = _dram->getEffectiveClusterSize();
    RowCount totalOutBufSizeDram = _dram->getTotalSpaceInOutputClusters();

    /**
     * 2. read the runs from SSD
     */
    std::vector<RunStreamer *> runStreamers;
    RowCount willFill = 0;
    for (int i = 0; i < runFiles.size(); i++) {
        std::string runFilename = runFiles[i].first;
        RowCount runSize = runFiles[i].second;
        printv("Run %d: %s, %lld records\n", i, runFilename.c_str(), runSize);
        // create a run reader and streamer
        RunReader *reader = new RunReader(runFilename, runSize, ssdPageSize);
        RowCount willRead = std::min((inBufSizePerRunDram * ssdPageSize), runSize);
        willFill += willRead;
        RunStreamer *runStreamer = new RunStreamer(reader, this, inBufSizePerRunDram);
        runStreamers.push_back(runStreamer);
    }
    printv("\t\twillFill %llu\n", willFill);
    flushv();

    // // 2. remaining runs fit in DRAM, merge them
    // LoserTree loserTree;
    // loserTree.constructTree(runStreamers);
    // printv("DEBUG: Merging %d runs in DRAM\n", runStreamers.size());
    // Record *head = new Record();
    // Record *current = head;
    // RowCount nSorted = 0;
    // RunWriter writer("merged.txt");
    // while (true) {
    //     Record *winner = loserTree.getNext();
    //     if (winner == NULL) {
    //         break;
    //     }
    //     flushv();
    //     current->next = winner;
    //     current = current->next;
    //     nSorted++;

    //     if (nSorted * Config::RECORD_SIZE >= totalOutBufSizeDram) {
    //         // write the sorted records to SSD
    //         Run merged(head->next, ssdPageSize);
    //         this->storeRun(merged);
    //         printv("DEBUG: Merged %lld records in DRAM\n", nSorted);
    //         // reset the head
    //         head = new Record();
    //         current = head;
    //     }
    // }
    // Record *merged = head->next;
    // Run mergedRun(merged, nSorted);
    // printv("DEBUG: Merged %lld records in DRAM\n", nSorted);

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
    : Storage(DRAM_NAME, Config::DRAM_CAPACITY, Config::DRAM_BANDWIDTH, Config::DRAM_LATENCY) {
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
    // printv("before spill: %s\n", this->reprUsageDetails().c_str());
    // printv("%s\n", outputStorage->reprUsageDetails().c_str());
#endif

    /**
     * 1. setup merging state
     */
    setupMergeStateForMiniruns(outputStorage->getPageSizeInRecords());
    RowCount totalInBufSizeDram = this->getTotalSpaceInInputClusters();
    printv("After setting up merging state: %s\n", this->reprUsageDetails().c_str());

    /**
     * 2. spill the runs that don't fit in _mergeFanInRec to SSD
     */
    RowCount keepNRecords = 0;
    int i = 0;
    for (; i < _miniruns.size(); i++) {
        int size = _miniruns[i].getSize();
        if (keepNRecords + size < totalInBufSizeDram) {
            keepNRecords += size;
        } else
            break;
    }
    /* transfer these runs to input buffer */
    _filledInputClusters += keepNRecords;
    _filled -= keepNRecords;
    // printv("After transferring %lld records to input buffer: %s\n", keepNRecords,
    //        this->reprUsageDetails().c_str());

    if (i < _miniruns.size()) {
        printv("DEBUG: spill %d runs out of %d to SSDs\n", _miniruns.size() - i, _miniruns.size());
        printv("\t\tspill from %dth runs to SSD\n", i);
        int spillNRecords = 0;
        int j;
        for (j = i; j < _miniruns.size(); j++) {
            spillNRecords += _miniruns[j].getSize();
            outputStorage->storeRun(_miniruns[j]);
            printv("\t\tSpilled run %d to SSD\n", j);
            flushv();
        }
        _miniruns.erase(_miniruns.begin() + i, _miniruns.end());

        printv("STATE -> %d cache-sized miniruns spilled to SSD\n", j - i);
        printv("ACCESS -> A write from RAM to SSD was made with size %llu bytes and latency %d "
               "ms\n",
               spillNRecords * Config::RECORD_SIZE, getAccessTimeInMillis(keepNRecords));
        // printv("after spill: %s\n", this->reprUsageDetails().c_str());
        // printv("%s\n", outputStorage->reprUsageDetails().c_str());
    } else {
        printv("DEBUG: All runs fit in DRAM\n");
    }

    /**
     * 3. remaining runs fit in DRAM, merge them
     * 4. when the merged run size fills the output buffer size, store the run in SSD
     */
    LoserTree loserTree;
    loserTree.constructTree(_miniruns);
    RunWriter *writer = outputStorage->getRunWriter();
    printv("STATE -> Merging %d cache-sized miniruns\n", _miniruns.size());

    Record *head = new Record();
    Record *current = head;
    RowCount nSorted = 0;
    RowCount runningCount = 0;
    while (true) {
        Record *winner = loserTree.getNext();
        if (winner == NULL) {
            break;
        }
        // printv("\t\tWinner: %s\n", winner->reprKey());
        flushv();
        // winner->next = nullptr;
        current->next = winner;
        current = current->next;
        nSorted++;
        runningCount++;
        if (nSorted > keepNRecords) { // verify the size
            printvv("ERROR: Merged run size exceeds %lld\n", keepNRecords);
            throw std::runtime_error("Merged run size exceeds");
        }
        if (runningCount >= _totalSpaceInOutputClusters) {
            /* write the sorted records to SSD */
            Run merged(head->next, runningCount);
            RowCount nRecord = writer->writeNextRun(merged);
            assert(nRecord == runningCount && "ERROR: Writing run to file");
            printv("\tACCESS -> A write to %s was made with size %llu bytes and latency %d ms\n",
                   outputStorage->getName().c_str(), runningCount * Config::RECORD_SIZE,
                   getAccessTimeInMillis(runningCount));
            /* reset the head */
            head->next = nullptr;
            current = head;
            runningCount = 0;
        }
    }
    if (runningCount > 0) {
        /* write the remaining records */
        Run merged(head->next, runningCount);
        RowCount nRecord = writer->writeNextRun(merged);
        assert(nRecord == runningCount && "ERROR: Writing remains of run to file");
        printv("\tACCESS -> A write to %s was made with size %llu bytes and latency %d ms\n",
               outputStorage->getName().c_str(), runningCount * Config::RECORD_SIZE,
               getAccessTimeInMillis(runningCount));
    }
    /* update the SSD used space */
    outputStorage->closeWriter(writer);
    printv("DEBUG: Merged %lld records in DRAM\n", nSorted);

#if defined(_VALIDATE)
    if (nSorted != keepNRecords) {
        printvv("ERROR: Merged run has %lld records, expected %lld\n", nSorted, keepNRecords);
    }
    assert(nSorted == keepNRecords && "VALIDATE: Merged run size mismatch");
    // validate the merged run
    Record *rec = head->next;
    while (rec != nullptr && rec->next != nullptr) {
        if (!(*rec < *rec->next)) {
            printvv("ERROR: Merged run is not sorted\n");
            printv("\t\t%s vs. %s\n", rec->reprKey(), rec->next->reprKey());
        }
        assert(*rec < *rec->next && "VALIDATE: Merged run is not sorted");
        rec = rec->next;
    }
    // printv("after merge: %s\n", this->reprUsageDetails().c_str());
    // printv("%s\n", outputStorage->reprUsageDetails().c_str());
#endif

    /**
     * 5. reset the DRAM and the merge state
     */
    this->reset();
    this->resetMergeState();
}
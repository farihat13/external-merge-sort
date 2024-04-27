
#include "StorageTypes.h"


// =========================================================
// -------------------------- Disk -------------------------
// =========================================================


HDD *HDD::instance = nullptr;


HDD::HDD(std::string name, ByteCount capacity, int bandwidth, double latency)
    : Storage(name, capacity, bandwidth, latency) {}

RowCount HDD::storeRun(Run &run) {
    RunWriter *writer = getRunWriter();
    RowCount nRecords = writer->writeNextRun(run);
    _filled += nRecords;
    closeWriter(writer);
    return nRecords;
}


void HDD::printStates(std::string where) {
    printv("\t\t------------ STATE_DETAILS: %s", where.c_str());
    printv("%s\n", DRAM::getInstance()->reprUsageDetails().c_str());
    printv("%s\n", SSD::getInstance()->reprUsageDetails().c_str());
    printv("%s\n", HDD::getInstance()->reprUsageDetails().c_str());
    // print stored runs in ssd and hdd
    SSD::getInstance()->printStoredRunFiles();
    HDD::getInstance()->printStoredRunFiles();
    printv("\t\t-------------------------\n");
    flushv();
}


int HDD::setupMergeStateInSSDAndDRAM() {
    DRAM *_dram = DRAM::getInstance();
    SSD *_ssd = SSD::getInstance();
    HDD *_hdd = this; // HDD::getInstance();

    RowCount _ssdPageSize = _ssd->getPageSizeInRecords();
    RowCount _hddPageSize = _hdd->getPageSizeInRecords();
    RowCount _ssdEmptySpace = _ssd->getTotalEmptySpaceInRecords();
    int minMergeFanOut = 2; // minimum fan-out for merge
    int _ssdRunFilesCount = _ssd->getRunfilesCount();
    int _hddRunFilesCount = _hdd->getRunfilesCount();

    int maxFetchFromHDDFanIn = (_ssdEmptySpace / _hddPageSize) - minMergeFanOut;
    maxFetchFromHDDFanIn = std::min(maxFetchFromHDDFanIn, _hddRunFilesCount);
    int fanIn = std::min(maxFetchFromHDDFanIn + _ssdRunFilesCount, _ssd->getMaxMergeFanIn());
    int fetchFromHDDFanIn = fanIn - _ssdRunFilesCount; // include all SSD runs to reduce disk IO
    printv("\t\t\tfanIn %d, fetchFromHDDFanIn %d\n", fanIn, fetchFromHDDFanIn);
    _ssd->setupMergeState(_hddPageSize, fetchFromHDDFanIn);
    printv("\t\t\tDEBUG: After setupSSD for merge in mergeHDDRuns: \n%s\n",
           _ssd->reprUsageDetails().c_str());

    if ((fanIn + 1) * _ssdPageSize > _dram->getCapacityInRecords()) {
        // TODO: avoid this during config
        printvv("ERROR: fanIn %d exceeds capacity %lld of DRAM\n", fanIn, getCapacityInRecords());
        throw std::runtime_error("Error: fanIn exceeds capacity in DRAM");
    }
    _dram->setupMergeState(_ssdPageSize, fanIn);
    printv("\t\t\tDEBUG: After setupDRAM for merge in mergeHDDRuns: %s\n",
           _dram->reprUsageDetails().c_str());

    return fanIn;
}

std::pair<std::vector<RunStreamer *>, RowCount> HDD::loadRunfilesToDRAM(size_t fanIn) {

    DRAM *_dram = DRAM::getInstance();
    SSD *_ssd = SSD::getInstance();
    HDD *_hdd = this; // HDD::getInstance();
    RowCount _ssdPageSize = _ssd->getPageSizeInRecords();
    RowCount _hddPageSize = _hdd->getPageSizeInRecords();
    // get the runfile names sorted by size ascending
    std::vector<std::pair<std::string, RowCount>> _ssdRunFiles =
        _ssd->runManager->getStoredRunsSortedBySize();
    std::vector<std::pair<std::string, RowCount>> _hddRunFiles =
        runManager->getStoredRunsSortedBySize();
    PageCount readAheadSSD = _ssd->getEffectiveClusterSize() / _hddPageSize;
    PageCount readAheadDRAM = _dram->getEffectiveClusterSize() / _ssdPageSize;
    printv("\t\t\treadAheadDram %d, readAheadSSD %d\n", readAheadDRAM, readAheadSSD);

    /**
     * load the runs to streamers from SSD using RunStreamer of RunReader
     */
    std::vector<RunStreamer *> runStreamers;
    RowCount allRunTotal = 0;
    size_t ithRunfile = 0;
    for (; ithRunfile < _ssdRunFiles.size(); ithRunfile++) {
        std::string runFilename = _ssdRunFiles[ithRunfile].first;
        RowCount runSize = _ssdRunFiles[ithRunfile].second;
        allRunTotal += runSize;
        printv("\t\t\t\tLoading run %d: %s, %lld records in mergeHDDRuns\n", ithRunfile,
               runFilename.c_str(), runSize);
        // create a run reader and streamer; the streamer will update the dram input buffer size
        RunReader *reader = new RunReader(runFilename, runSize, _ssdPageSize);
        RunStreamer *runStreamer =
            new RunStreamer(StreamerType::READER, reader, _ssd, _dram, readAheadDRAM);
        runStreamers.push_back(runStreamer);
    }
    printv("\t\t\tDEBUG: After loading SSDRunfiles in mergeHDDRuns: \n%s\n",
           _dram->reprUsageDetails().c_str());
    printv("%s\n", _ssd->reprUsageDetails().c_str());
    flushv();

    /**
     * load the runs to streamers from HDD using RunStreamer of (RunStreamer of RunReader)
     */
    if (ithRunfile < fanIn) {
        int i = 0;
        for (; ithRunfile < fanIn; ithRunfile++) {
            std::string runFilename = _hddRunFiles[i].first;
            RowCount runSize = _hddRunFiles[i++].second;
            allRunTotal += runSize;
            printv("\t\t\t\tLoading run %d: %s, %lld records in mergeHDDRuns\n", ithRunfile,
                   runFilename.c_str(), runSize);
            // create a run reader and streamer; the run streamer will update the dram input buffer
            // size
            RunReader *reader = new RunReader(runFilename, runSize, _hddPageSize);
            RunStreamer *rsInner =
                new RunStreamer(StreamerType::READER, reader, _hdd, _ssd, readAheadSSD, true);
            RunStreamer *rsOuter =
                new RunStreamer(StreamerType::STREAMER, rsInner, _ssd, _dram, readAheadDRAM);
            runStreamers.push_back(rsOuter);

            // // ------------------------- TODO: remove this
            // printv("\t\t\tFOCUS: After loading HDDRunfiles in mergeHDDRuns: %s\n",
            //        _dram->reprUsageDetails().c_str());
            // printv("%s\n", _ssd->reprUsageDetails().c_str());
            // printv("%s\n", _hdd->reprUsageDetails().c_str());
            // flushv();
            // RowCount nRecords = 0;
            // Record *rec = rsOuter->getCurrRecord();
            // while (rec != nullptr) {
            //     nRecords++;
            //     if (nRecords > runSize) {
            //         printvv("ERROR: Loaded more records %lld than runSize %lld\n", nRecords,
            //                 runSize);
            //         throw std::runtime_error("Loaded more records than runSize");
            //     }
            //     printv("\t\t\t\t[%d]: %s\n", nRecords, rec->reprKey());
            //     rec = rsOuter->moveNext();
            // }
            // printv("\t\t\t\tLoaded %lld records from %s\n", nRecords, runFilename.c_str());
            // exit(0); // TODO: remove this
            // -------------------------
        }
        printv("\t\t\tDEBUG: After loading HDDRunfiles in mergeHDDRuns: %s\n",
               _dram->reprUsageDetails().c_str());
        printv("%s\n", _ssd->reprUsageDetails().c_str());
        printv("%s\n", _hdd->reprUsageDetails().c_str());
        flushv();
    }
    printv("\t\t\tAllRunTotal: %lld\n", allRunTotal);

    return std::make_pair(runStreamers, allRunTotal);
}

void HDD::mergeHDDRuns() {

    DRAM *_dram = DRAM::getInstance();
    SSD *_ssd = SSD::getInstance();
    HDD *_hdd = this; // HDD::getInstance();

    // print all device information
    printStates("DEBUG: before mergeHDDRuns");

    RowCount _ssdPageSize, _hddPageSize, _ssdEmptySpace;
    _ssdPageSize = SSD::getInstance()->getPageSizeInRecords();
    _hddPageSize = HDD::getInstance()->getPageSizeInRecords();
    _ssdEmptySpace = SSD::getInstance()->getTotalEmptySpaceInRecords();
    printv("\t\t\t_ssdPageSize %lld, _hddPageSize %lld\n", _ssdPageSize, _hddPageSize);

    /**
     * 1. verify _ssd is reasonably empty; at least 2 runs from hdd should fit in ssd
     * and at least 2 output buffers should fit in ssd
     */
    int minMergeFanIn = 2;  // minimum fan-in for merge
    int minMergeFanOut = 2; // minimum fan-out for merge
    if (_hddPageSize * (minMergeFanIn + minMergeFanOut) > _ssdEmptySpace) {
        printv("WARNING: SSD is almost full, merging SSD runs first\n");
        _ssd->mergeSSDRuns(_hdd);
        return;
    }

    /**
     * 2. adjust the fanIn based on the available space in SSD
     * and setup the input and output buffer sizes in SSD and DRAM
     */
    int fanIn = setupMergeStateInSSDAndDRAM();

    /**
     * 3. load the runs to streamers from SSD and HDD using RunReaders and RunStreamers
     */
    auto pair = loadRunfilesToDRAM(fanIn);
    std::vector<RunStreamer *> runStreamers = pair.first;
    RowCount allRunTotal = pair.second;
    printv("\t\t\t#runStreamers %d, allRunTotal %lld\n", runStreamers.size(), allRunTotal);
    std::vector<std::string> filesToRemove;
    for (auto runStreamer : runStreamers) {
        filesToRemove.push_back(runStreamer->getFilename());
    }

    /**
     * 4. merge the runs in DRAM
     */
    RowCount totalOutBufSizeDram = _dram->getTotalSpaceInOutputClusters();
    LoserTree loserTree;
    loserTree.constructTree(runStreamers);
    RunWriter *writer = _ssd->getRunWriter();

    Record *head = new Record();
    Record *current = head;
    RowCount nSorted = 0;
    RowCount runningCount = 0;
    while (true) {
        // loserTree.printTree();
        Record *winner = loserTree.getNext();
        if (winner == NULL) { break; }
        // printv("rc: %d, winner %s\n", runningCount, winner->reprKey());
        // flushv();
        current->next = winner;
        current = current->next;
        nSorted++;
        runningCount++;
        if (nSorted > allRunTotal) {
            printvv("ERROR: Merged run size exceeds %lld\n", allRunTotal);
            throw std::runtime_error("Merged run size exceeds");
        }
        // if (runningCount > 10) {
        //     exit(0); // TODO: remove this
        // }
        if (runningCount >= totalOutBufSizeDram) {
            // printv("runningCount %lld, nSorted %lld, totalOutBufSizeDram %lld\n", runningCount,
            //    nSorted, totalOutBufSizeDram);
            // flushv();
            // write the sorted records to SSD
            Run merged(head->next, runningCount);
            RowCount nRecord = _ssd->writeNextChunk(writer, merged);
            printv("\t\t\tSTATE -> Merging runs, writing %llu records in output buffer to %s\n",
                   runningCount, writer->getFilename().c_str());
            printv("\t\t\tACCESS -> A write to SSD was made with size %llu bytes and "
                   "latency %d ms\n",
                   runningCount * Config::RECORD_SIZE, getSSDAccessTime(runningCount));
            // printv("FOCUS: %s\n", _ssd->reprUsageDetails().c_str());
            if (nRecord != runningCount) {
                printvv("ERROR: Writing run to file: nRec %lld != runningCount %lld\n", nRecord,
                        runningCount);
            }
            assert(nRecord == runningCount && "ERROR: Writing run to file");
            // reset the head
            head->next = nullptr;
            current = head;
            runningCount = 0;
        }
        flushv();
    }
    if (runningCount > 0) {
        /* write the remaining records */
        Run merged(head->next, runningCount);
        RowCount nRecord = _ssd->writeNextChunk(writer, merged);
        printv("\t\t\tSTATE -> Merged runs, writing final output buffer to %s\n",
               writer->getFilename().c_str());
        printv("\t\t\tACCESS -> A write to SSD was made with size %llu bytes and latency %d ms\n",
               runningCount * Config::RECORD_SIZE, getSSDAccessTime(runningCount));
        printv("FOCUS: %s\n", _ssd->reprUsageDetails().c_str());
        assert(nRecord == runningCount && "ERROR: Writing run to file");
        // room for optimization
    }
    flushv();

    /**
     * 5.
     * -> close the RunWriter that was storing the merged run, the SSD used space should be updated
     *by the writeNextChunk,
     * -> delete the run file entries from the run manager the actual files has already been deleted
     *by the runreader and endspillsession
     * -> reset the dram, it should be empty now
     **/
    _ssd->closeWriter(writer);
    for (auto runFilename : filesToRemove) {
        bool removed = _ssd->runManager->removeRunFile(runFilename);
        if (!removed) { // this is a file from HDD
            _hdd->runManager->removeRunFile(runFilename);
            printv("\t\t\t\tRemoved run file %s from HDD\n", runFilename.c_str());
        } else {
            printv("\t\t\t\tRemoved run file %s from SSD\n", runFilename.c_str());
        }
    }
    for (auto streamer : runStreamers) {
        delete streamer;
    }
    // reset the dram
    _dram->reset();
    // print all device information
    printv("\t\t\tINFO: MERGE_HDD_RUNS COMPLETE %lld records in SSD\n", nSorted);
    printStates("DEBUG: after mergeHDDRuns:");
    flushv();

    assert(nSorted == allRunTotal && "ERROR: Merged run size mismatch in mergeHDDRuns");

    // exit(0); // TODO: remove this
}


// =========================================================
// -------------------------- SSD -------------------------
// =========================================================


SSD *SSD::instance = nullptr;

SSD::SSD() : HDD(SSD_NAME, Config::SSD_CAPACITY, Config::SSD_BANDWIDTH, Config::SSD_LATENCY) {
    this->setSpillTo(HDD::getInstance());
}


void SSD::setupMergeState(RowCount outputDevicePageSize, int fanIn) {
    RowCount _emptySpace = getTotalEmptySpaceInRecords();
    if ((fanIn + 1) * outputDevicePageSize > _emptySpace) { // should not occur
        printvv("ERROR: fanIn %d exceeds available space %lld in SSD\n", fanIn, _emptySpace);
        throw std::runtime_error("Error: fanIn exceeds available space in SSD");
    }

    /**
     * 1. calculate output buffer size
     */
    RowCount totalOutputBufferSpaceOpt1 = getMaxMergeFanOut() * outputDevicePageSize;
    RowCount totalOutputBufferSpaceOpt2 =
        _emptySpace - fanIn * outputDevicePageSize; // maximum available space for output buffer
    _totalSpaceInOutputClusters = std::min(totalOutputBufferSpaceOpt1, totalOutputBufferSpaceOpt2);

    printv("\t\t\t\tOutputBuf: Opt1 %lld, Opt2 %lld, MaxFanOut %d\n", totalOutputBufferSpaceOpt1,
           totalOutputBufferSpaceOpt2, getMaxMergeFanOut());

    /**
     * 2. calculate input buffer size
     */
    _totalSpaceInInputClusters = _emptySpace - _totalSpaceInOutputClusters;
    // calculate per run input buffer size
    _effectiveClusterSize = RoundDown(_totalSpaceInInputClusters / fanIn, outputDevicePageSize);
    // make per run input buffer size a multiple of output device page size
    _totalSpaceInInputClusters = _effectiveClusterSize * fanIn;

    /**
     * 3. recalculate output buffer size to include the remaining space
     */
    _totalSpaceInOutputClusters =
        RoundDown(_emptySpace - _totalSpaceInInputClusters, outputDevicePageSize);

    /**
     * 4. reset the input and output buffer sizes
     */
    _filledInputClusters = 0;
    _filledOutputClusters = 0;
}


/**
 * assumption: this function is called during the first pass of the sort
 * after this call, all the runs in the SSD will be merged and stored in HDD
 * the SSD will be empty
 */
void SSD::mergeSSDRuns(HDD *outputDevice) {
    TRACE(true);

    DRAM *_dram = DRAM::getInstance();
    SSD *_ssd = this; // SSD::getInstance();
    // HDD *_hdd = outputDevice; // HDD::getInstance();

    // print all device information
    printStates("DEBUG: before mergeSSDRuns\n");

    /** assert: the total records in the run manager is equal to the filled records */
    assert(this->runManager->getTotalRecords() == this->_filled);
    /** assert;  the total records in the run manager does not exceed merge fan-in */
    if (this->_filled > getMergeFanInRecords()) {
        printvv("ERROR: Run size %lld exceeds merge fan-in %lld\n", this->_filled,
                getMergeFanInRecords());
        throw std::runtime_error("Run size exceeds merge fan-in in " + this->getName());
    }

    // get the constants
    RowCount ssdPageSize = _ssd->getPageSizeInRecords();
    /** get the runfile names sorted by size ascending */
    std::vector<std::pair<std::string, RowCount>> runFiles =
        runManager->getStoredRunsSortedBySize();

    /**
     * 1. setup merging state
     */
    int fanIn = runFiles.size();
    if ((fanIn + 1) * ssdPageSize > _dram->getCapacityInRecords()) {
        printvv("ERROR: fanIn %d exceeds capacity %lld of DRAM\n", fanIn, getCapacityInRecords());
        throw std::runtime_error("Error: fanIn exceeds capacity in storage setup");
    }
    _dram->setupMergeState(ssdPageSize, fanIn);
    printv("\t\t\tDEBUG: After setting up merging state in mergeSSDRuns: %s\n",
           _dram->reprUsageDetails().c_str());
    RowCount inBufSizePerRunDram = _dram->getEffectiveClusterSize();
    RowCount totalOutBufSizeDram = _dram->getTotalSpaceInOutputClusters();
    PageCount readAhead = inBufSizePerRunDram / ssdPageSize;
    printv("\t\t\tDEBUG: inBufSizePerRunDram %lld, totalOutBufSizeDram %lld, readAhead %d\n",
           inBufSizePerRunDram, totalOutBufSizeDram, readAhead);

    /**
     * 2. read the runs from SSD
     */
    printv("\t\tSTATE -> MERGE_SSD_RUNS: merging %d runs from SSD\n", runFiles.size());
    std::vector<RunStreamer *> runStreamers;
    for (size_t i = 0; i < runFiles.size(); i++) {
        std::string runFilename = runFiles[i].first;
        RowCount runSize = runFiles[i].second;
        printv("\t\t\t\tLoading run %d: %s, %lld records in mergeSSDRuns\n", i, runFilename.c_str(),
               runSize);
        // create a run reader and streamer
        RunReader *reader = new RunReader(runFilename, runSize, ssdPageSize);
        // the run streamer will update the dram input buffer size
        RunStreamer *runStreamer =
            new RunStreamer(StreamerType::READER, reader, _ssd, _dram, readAhead);
        runStreamers.push_back(runStreamer);
    }
    printv("\t\t\tDEBUG: After loading runs to streamers in mergeSSDRuns: %s\n",
           _dram->reprUsageDetails().c_str());
    printv("%s", _ssd->reprUsageDetails().c_str());
    flushv();


    /**
     * 3. loaded runs fit in DRAM, merge them
     * 4. when the merged run size fills the output buffer size, store the run in HDD
     */
    RowCount allRunTotal = this->runManager->getTotalRecords();
    LoserTree loserTree;
    loserTree.constructTree(runStreamers);
    RunWriter *writer = _ssd->getRunWriter();

    Record *head = new Record();
    Record *current = head;
    RowCount nSorted = 0;
    RowCount runningCount = 0;
    while (true) {
        Record *winner = loserTree.getNext();
        if (winner == NULL) { break; }
        flushv();
        current->next = winner;
        current = current->next;
        nSorted++;
        runningCount++;
        if (nSorted > allRunTotal) {
            printvv("ERROR: Merged run size exceeds %lld\n", allRunTotal);
            throw std::runtime_error("Merged run size exceeds");
        }
        if (runningCount >= totalOutBufSizeDram) {
            // write the sorted records to SSD
            Run merged(head->next, runningCount);
            RowCount nRecord = _ssd->writeNextChunk(writer, merged);
            printv("\t\t\tSTATE -> Merging runs, writing %llu records in output buffer to %s\n",
                   runningCount, writer->getFilename().c_str());
            printv(
                "\t\t\tACCESS -> A write to SSD was made with size %llu bytes and latency %d ms\n",
                runningCount * Config::RECORD_SIZE, getSSDAccessTime(runningCount));
            if (nRecord != runningCount) {
                printvv("ERROR: Writing run to file: nRec %lld != runningCount %lld\n", nRecord,
                        runningCount);
            }
            assert(nRecord == runningCount && "ERROR: Writing run to file");
            // reset the head
            head->next = nullptr;
            current = head;
            runningCount = 0;
        }
        flushv();
    }
    if (runningCount > 0) {
        /* write the remaining records */
        Run merged(head->next, runningCount);
        RowCount nRecord = _ssd->writeNextChunk(writer, merged);
        printv("\t\t\tSTATE -> Merged runs, writing final output buffer to %s\n",
               writer->getFilename().c_str());
        printv("\t\t\tACCESS -> A write to SSD was made with size %llu bytes and latency %d ms\n",
               runningCount * Config::RECORD_SIZE, getSSDAccessTime(runningCount));
        assert(nRecord == runningCount && "ERROR: Writing run to file");
        // room for optimization
    }
    /**
     * 5.
     * -> close the RunWriter that was storing the merged run, the SSD used space should be updated
     *by the writeNextChunk,
     * -> delete the run file entries from the run manager the actual files has already been deleted
     *by the runreader and endspillsession
     * -> reset the dram, it should be empty now
     **/
    _ssd->closeWriter(writer);
    for (auto runFile : runFiles) {
        std::string runFilename = runFile.first;
        this->runManager->removeRunFile(runFilename);
    }
    _dram->reset();

    // print all device information
    printv("\t\t\tINFO: MERGE_SSD_RUNS COMPLETE %lld records in SSD\n", nSorted);
    printStates("DEBUG: after mergeSSDRuns:");
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
    this->spillTo = SSD::getInstance();
    this->reset();
}

/**
 * @brief Setup merging state for the storage device (used for merging miniruns)
 * Since, fanIn is not provided, it will use MERGE_FAN_OUT as fanOut
 * and calculate totalInputClusterSize based on outputDevicePageSize
 * NOTE: the _effectiveClusterSize will be set to -1, don't use it
 * @return the fanOut value used
 */
int DRAM::setupMergeStateForMiniruns(RowCount outputDevicePageSize) {
    // NOTE: don't use getTotalEmptySpaceInRecords() here, since the dram is already filled
    _totalSpaceInOutputClusters =
        RoundUp(getClusterSize() * getPageSizeInRecords(), outputDevicePageSize);
    _totalSpaceInInputClusters =
        RoundDown(getCapacityInRecords() - _totalSpaceInOutputClusters, outputDevicePageSize);
    _totalSpaceInOutputClusters =
        RoundDown(getCapacityInRecords() - _totalSpaceInInputClusters, outputDevicePageSize);
    _effectiveClusterSize = _totalSpaceInOutputClusters / getMaxMergeFanOut();
    _filledInputClusters = 0;
    _filledOutputClusters = 0;
    return getMaxMergeFanOut();
}

void DRAM::setupMergeState(RowCount outputDevicePageSize, int fanIn) {
    assert(_filled == 0 && "ERROR: DRAM is not empty");
    /** assumption: the following check will be done before calling this */
    if ((fanIn + 1) * outputDevicePageSize > getCapacityInRecords()) {
        printvv("ERROR: fanIn %d exceeds capacity %lld\n", fanIn, getCapacityInRecords());
        throw std::runtime_error("Error: fanIn exceeds capacity in storage setup");
    }

    RowCount maxFanOut = getCapacityInRecords() - outputDevicePageSize * fanIn;
    _totalSpaceInOutputClusters = std::min(maxFanOut, getMergeFanOutRecords());
    _totalSpaceInInputClusters = getCapacityInRecords() - _totalSpaceInOutputClusters;
    _effectiveClusterSize = RoundDown(_totalSpaceInInputClusters / fanIn, outputDevicePageSize);
    if (_effectiveClusterSize < outputDevicePageSize) { /** this should not occur */
        printvv("ERROR: effective cluster size %lld < output device page size %lld\n",
                _effectiveClusterSize, outputDevicePageSize);
        throw std::runtime_error("Error: effective cluster size < output device page size"
                                 " in storage setup");
    }
    // recalculate based on effective cluster size
    _totalSpaceInInputClusters = _effectiveClusterSize * fanIn;
    _totalSpaceInOutputClusters =
        RoundDown(getCapacityInRecords() - _totalSpaceInInputClusters, outputDevicePageSize);

    _filledInputClusters = 0;
    _filledOutputClusters = 0;
}


RowCount DRAM::loadInput(RowCount nRecordsToRead) {
    TRACE(true);
    /**
     * 1. read records from HDD to DRAM
     **/
    char *recordsData = HDD::getInstance()->readRecords(&nRecordsToRead);
    if (recordsData == NULL || nRecordsToRead == 0) { printvv("WARNING: no records read\n"); }
    /**
     * 2. create a linked list of records
     **/
    Record *tail = nullptr;
    for (RowCount i = 0; i < nRecordsToRead; i++) {
        Record *rec = new Record(recordsData);
        if (_head == NULL) {
            _head = rec;
            tail = rec;
        } else {
            tail->next = rec;
            tail = rec;
        }
        // update
        recordsData += Config::RECORD_SIZE;
    }
    /**
     * 3. update DRAM usage
     */
    _filled += nRecordsToRead;

    // print debug information
    printv("\t\t\tSTATE -> %llu input records read from HDD to DRAM\n", nRecordsToRead);
    printv(
        "\t\t\tACCESS -> A read from HDD to RAM was made with size %llu bytes and latency %d ms\n",
        nRecordsToRead * Config::RECORD_SIZE, getHDDAccessTime(nRecordsToRead));
    printv("%s\n", this->reprUsageDetails().c_str());
    printv("\t\t\tDEBUG: LOADED_INPUT: Loaded %lld records to DRAM\n", nRecordsToRead);
    flushv();
    return nRecordsToRead;
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
    printv("\t\t\tVALIDATE: %lld out of %lld records loaded in RAM\n", count, nRecords);
    printv("\t\t\tDRAM filled %llu out of %llu Records\n", count,
           Config::DRAM_CAPACITY / Config::RECORD_SIZE);
    flushv();
    assert(count == nRecords && "VALIDATE: Invalid number of records loaded in RAM");
    assert(count <= Config::DRAM_CAPACITY / Config::RECORD_SIZE &&
           "\t\t\tVALIDATE: Number of records exceeds DRAM capacity");
#endif

    // sort the records in cache
    RowCount _cacheSize = Config::CACHE_SIZE / Config::RECORD_SIZE;
    assert(_miniruns.size() == 0 && "ERROR: miniruns is not empty");
    // std::vector<int> runSizes;
    Record *curr = _head;
    for (RowCount i = 0; i < nRecords; i += _cacheSize) {
        std::vector<Record *> records;
        for (RowCount j = 0; j < _cacheSize && curr != nullptr; j++) {
            records.push_back(curr);
            curr = curr->next;
        }
        quickSort(records);
        // update the next pointer
        for (size_t j = 0; j < records.size() - 1; j++) {
            records[j]->next = records[j + 1];
        }
        records.back()->next = nullptr;
        // create a run
        Run run(records[0], records.size());
        _miniruns.push_back(run);
    }
    printv("\t\t\tDEBUG: Sorted %lld records and generated %d runs in DRAM\n", nRecords,
           _miniruns.size());
    flushv();
}

void DRAM::mergeMiniRuns(HDD *outputStorage) {
    TRACE(true);
#if defined(_VALIDATE)
    // validate each run is sorted and the size of the run
    for (size_t i = 0; i < _miniruns.size(); i++) {
        Record *rec = _miniruns[i].getHead();
        size_t count = 1;
        // printv("\t\t\tRun %d: %d records\n", i, _miniruns[i].getSize());
        // printv("\t\t\t%s\n", rec->reprKey());
        while (rec->next != nullptr) {
            // printv("\t\t%s\n", rec->next->reprKey());
            assert(*rec < *rec->next && "VALIDATE: Run is not sorted");
            rec = rec->next;
            count++;
        }
        // printvv("\t\t\tVALIDATE: Run %d: %d records out of %d records\n", i, count,
        //         _miniruns[i].getSize());
        assert(count == _miniruns[i].getSize() && "VALIDATE: Run size mismatch");
    }
    // printv("\t\t\tbefore spill: %s\n", this->reprUsageDetails().c_str());
    // printv("\t\t\t%s\n", outputStorage->reprUsageDetails().c_str());
#endif

    /**
     * 1. setup merging state
     * -> uses the default FAN_IN FAN_OUT ratios
     * -> calculate the total input buffer size and the total output buffer size in DRAM
     * -> calculate the effective cluster size
     * -> sets the filled input and output clusters to 0
     */
    setupMergeStateForMiniruns(outputStorage->getPageSizeInRecords());
    printv("\t\t\tAfter setting up merging state in mergeMini: %s\n",
           this->reprUsageDetails().c_str());

    /**
     * 2. spill the runs that don't fit in `_filledInputClusters` to SSD
     */
    RowCount totalInBufSizeDram = this->getTotalSpaceInInputClusters();
    RowCount keepNRecordsInDRAM = 0;
    size_t i = 0;
    for (; i < _miniruns.size(); i++) {
        int size = _miniruns[i].getSize();
        if (keepNRecordsInDRAM + size < totalInBufSizeDram) {
            keepNRecordsInDRAM += size;
        } else {
            break;
        }
    }
    /** emulate transfer of these runs to input buffer */
    _filledInputClusters += keepNRecordsInDRAM;
    _filled -= keepNRecordsInDRAM;

    if (i < _miniruns.size()) {
        printv("\t\t\tDEBUG: spill %d runs out of %d to SSDs starting from %dth run in mergeMini\n",
               _miniruns.size() - i, _miniruns.size(), i);
        int spillNRecords = 0;
        size_t j;
        for (j = i; j < _miniruns.size(); j++) {
            spillNRecords += _miniruns[j].getSize();
            outputStorage->storeRun(_miniruns[j]);
            // printv("\t\t\t\tSpilled run %d to SSD\n", j);
            flushv();
        }
        _miniruns.erase(_miniruns.begin() + i, _miniruns.end());

        printv("\t\t\tSTATE -> %d cache-sized miniruns spilled to SSD\n", j - i);
        printv("\t\t\tACCESS -> A write to SSD was made with size %llu bytes and latency %d ms\n",
               spillNRecords * Config::RECORD_SIZE, getSSDAccessTime(spillNRecords));
    } else {
        printv("\t\t\tDEBUG: All miniruns fit in DRAM\n");
    }
    flushv();

    /**
     * 3. remaining runs fit in DRAM, merge them
     * 4. when the merged run size fills the output buffer size, store the run in SSD
     */
    LoserTree loserTree;
    loserTree.constructTree(_miniruns);
    RunWriter *writer = outputStorage->getRunWriter();
    printv("\t\t\tSTATE -> Merging %d cache-sized miniruns\n", _miniruns.size());

    Record *head = new Record();
    Record *current = head;
    RowCount nSorted = 0;
    RowCount runningCount = 0;
    while (true) {
        Record *winner = loserTree.getNext();
        if (winner == NULL) { break; }
        // printv("\t\tWinner: %s\n", winner->reprKey());
        flushv();
        // winner->next = nullptr;
        current->next = winner;
        current = current->next;
        nSorted++;
        runningCount++;
        if (nSorted > keepNRecordsInDRAM) { // verify the size
            printvv("ERROR: Merged run size exceeds %lld\n", keepNRecordsInDRAM);
            throw std::runtime_error("Merged run size exceeds");
        }
        if (runningCount >= _totalSpaceInOutputClusters) {
            /* write the sorted records to SSD */
            Run merged(head->next, runningCount);
            RowCount nRecord = outputStorage->writeNextChunk(writer, merged);
            assert(nRecord == runningCount && "ERROR: Writing run to file");
            printv(
                "\t\t\tACCESS -> A write to %s was made with size %llu bytes and latency %d ms\n",
                outputStorage->getName().c_str(), runningCount * Config::RECORD_SIZE,
                outputStorage->getAccessTimeInMillis(runningCount));
            /* reset the head */
            head->next = nullptr;
            current = head;
            runningCount = 0;
        }
    }
    if (runningCount > 0) {
        /* write the remaining records */
        printv("\t\t\tWriting the remaining %lld records to SSD\n", runningCount);
        Run merged(head->next, runningCount);
        RowCount nRecord = outputStorage->writeNextChunk(writer, merged);
        assert(nRecord == runningCount && "ERROR: Writing remains of run to file");
        printv("\t\t\tACCESS -> A write to %s was made with size %llu bytes and latency %d ms\n",
               outputStorage->getName().c_str(), runningCount * Config::RECORD_SIZE,
               outputStorage->getAccessTimeInMillis(runningCount));
    }
    outputStorage->closeWriter(writer);
    assert(outputStorage->getTotalFilledSpaceInRecords() >= nSorted &&
           "ERROR: outputStorage filled space mismatch");

#if defined(_VALIDATE)
    if (nSorted != keepNRecordsInDRAM) {
        printvv("ERROR: Merged run has %lld records, expected %lld\n", nSorted, keepNRecordsInDRAM);
    }
    assert(nSorted == keepNRecordsInDRAM && "VALIDATE: Merged run size mismatch");
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
#endif

    /**
     * 5. reset the DRAM and the merge state
     * The DRAM should be empty now
     */
    this->reset();
    this->resetMergeState();

    // cleanup memory
    delete head;

    // final print
    printv("\t\t\tINFO: MERGE_MINIRUNS Complete: Merged %lld records in DRAM\n", nSorted);
    flushv();
}


// =============================================================================
// ------------------------------ CommonFunctions ------------------------------
// =============================================================================


int getDRAMAccessTime(RowCount nRecords) {
    return (int)(DRAM::getInstance()->getAccessTimeInSec(nRecords) * 1000);
}

int getSSDAccessTime(RowCount nRecords) {
    return (int)(SSD::getInstance()->getAccessTimeInSec(nRecords) * 1000);
}

int getHDDAccessTime(RowCount nRecords) {
    return (int)(HDD::getInstance()->getAccessTimeInSec(nRecords) * 1000);
}

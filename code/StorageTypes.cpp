
#include "StorageTypes.h"


// =========================================================
// -------------------------- Disk -------------------------
// =========================================================


HDD *HDD::instance = nullptr;


HDD::HDD(std::string name, ByteCount capacity, int bandwidth, double latency)
    : Storage(name, capacity, bandwidth, latency) {}

RowCount HDD::storeRun(Run *run) {
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

    RowCount _hddPageSize, _ssdEmptySpace;
    _hddPageSize = HDD::getInstance()->getPageSizeInRecords();
    _ssdEmptySpace = SSD::getInstance()->getTotalEmptySpaceInRecords();
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

    printvv("\tMERGE_HDD_RUNS START: Merging %d runs\n", fanIn);

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
     * 4. merge the runs using a loser tree
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
        Record *winner = loserTree.getNext();
        if (winner == NULL) { break; }
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
            Run *merged = new Run(head->next, runningCount);
            RowCount nRecord = _ssd->writeNextChunk(writer, merged);
            printvv("\t\tSTATE -> Merging runs, Spill to %s %lld records\n",
                    writer->getFilename().c_str(), runningCount);
            printvv("\t\tACCESS -> A write to SSD was made with size %llu bytes and "
                    "latency %d ms\n",
                    runningCount * Config::RECORD_SIZE, getSSDAccessTime(runningCount));
            flushv();
            if (nRecord != runningCount) {
                printvv("ERROR: Writing run to file: nRec %lld != runningCount %lld\n", nRecord,
                        runningCount);
            }
            assert(nRecord == runningCount && "ERROR: Writing run to file");
            // free memory
            delete merged;
            // reset the head
            head->next = nullptr;
            current = head;
            runningCount = 0;
        }
        flushv();
    }
    if (runningCount > 0) {
        /* write the remaining records */
        Run *merged = new Run(head->next, runningCount);
        RowCount nRecord = _ssd->writeNextChunk(writer, merged);
        printvv("\t\tSTATE -> Merged runs, Spill to %s %lld records\n",
                writer->getFilename().c_str(), runningCount);
        printvv("\t\tACCESS -> A write to SSD was made with size %llu bytes and latency %d ms\n",
                runningCount * Config::RECORD_SIZE, getSSDAccessTime(runningCount));
        flushv();
        assert(nRecord == runningCount && "ERROR: Writing run to file");
        // free memory
        delete merged;
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
    // reset the dram
    _dram->reset();
    // free memory
    // for (auto streamer : runStreamers) {
    //     delete streamer;
    // }
    delete head;

    assert(nSorted == allRunTotal && "ERROR: Merged run size mismatch in mergeHDDRuns");
    // print all device information
    printStates("DEBUG: after mergeHDDRuns:");
    printvv("\tMERGE_HDD_RUNS COMPLETE: Merged %lld records\n", nSorted);
    flushvv();
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
    // TRACE(true);
    // print all device information
    printStates("DEBUG: before mergeSSDRuns\n");

    DRAM *_dram = DRAM::getInstance();
    SSD *_ssd = SSD::getInstance();
    /**
     * 1. verify the SSD has space for output buffers
     */
    assert(_ssd->runManager->getTotalRecords() == _ssd->_filled);
    if (_ssd->_filled > _ssd->getMergeFanInRecords()) {
        // TODO: transfer the runs to HDD
        std::string msg = "ERROR: Runs occupy " + std::to_string(_ssd->_filled) +
                          " records, exceeds merge fan-in" +
                          std::to_string(_ssd->getMergeFanInRecords());
        printvv("%s\n", msg.c_str());
        throw std::runtime_error(msg);
    }

    // get configuration parameters
    RowCount _ssdPageSize = _ssd->getPageSizeInRecords();
    std::vector<std::pair<std::string, RowCount>> runFiles =
        runManager->getStoredRunsSortedBySize();
    /**
     * 2. setup merging state in dram
     */
    int fanIn = runFiles.size();
    if ((fanIn + 1) * _ssdPageSize > _dram->getCapacityInRecords()) {
        std::string msg = "ERROR: fanIn " + std::to_string(fanIn) + " exceeds capacity " +
                          std::to_string(_dram->getCapacityInRecords()) + " of DRAM";
        printvv("%s\n", msg.c_str());
        throw std::runtime_error(msg);
    }
    _dram->setupMergeState(_ssdPageSize, fanIn);
    printv("\t\t\tDEBUG: After setting up merging state in mergeSSDRuns: %s\n",
           _dram->reprUsageDetails().c_str());

    // get configuration parameters
    RowCount _inBufSizePerRunDRAM = _dram->getEffectiveClusterSize();
    RowCount _totalOutBufSizeDRAM = _dram->getTotalSpaceInOutputClusters();
    PageCount _readAheadDRAM = _inBufSizePerRunDRAM / _ssdPageSize;
    printv("\t\t\tinBufSizePerRunDram %lld, totalOutBufSizeDram %lld, readAhead %d\n",
           _inBufSizePerRunDRAM, _totalOutBufSizeDRAM, _readAheadDRAM);
    /**
     * 3. load the runs from SSD to DRAM
     */
    printvv("\tMERGE_SSD_RUNS START: Merging %d runs\n", runFiles.size());
    std::vector<RunStreamer *> runStreamers;
    for (size_t i = 0; i < runFiles.size(); i++) {
        std::string runFilename = runFiles[i].first;
        RowCount runSize = runFiles[i].second;
        /* create a run reader and streamer; the run streamer will update the dram input bufsize */
        RunReader *reader = new RunReader(runFilename, runSize, _ssdPageSize);
        RunStreamer *runStreamer =
            new RunStreamer(StreamerType::READER, reader, _ssd, _dram, _readAheadDRAM);
        runStreamers.push_back(runStreamer);
    }
    printv("\t\t\tDEBUG: After loading runs to streamers in mergeSSDRuns: %s\n",
           _dram->reprUsageDetails().c_str());
    printv("%s", _ssd->reprUsageDetails().c_str());
    flushv();


    /**
     * 4. Merge the runs using a loser tree
     */
    // setup the loser tree
    RowCount allRunTotal = this->runManager->getTotalRecords();
    LoserTree loserTree;
    loserTree.constructTree(runStreamers);
    // start merging the runs
    RunWriter *writer = _ssd->getRunWriter();
    Record *head = new Record();
    Record *current = head;
    RowCount nSorted = 0;
    RowCount runningCount = 0;
    while (true) {
        Record *winner = loserTree.getNext();
        if (winner == nullptr) { break; }
        current->next = winner;
        current = current->next;
        nSorted++;
        runningCount++;
        if (nSorted > allRunTotal) {
            printvv("ERROR: Merged run size exceeds %lld\n", allRunTotal);
            throw std::runtime_error("Merged run size exceeds");
        }
        if (runningCount >= _totalOutBufSizeDRAM) {
            /**
             * 4.1. when the merged run size fills the DRAM output buffer size, spill the run to
             * SSD; when the SSD output buffer size is filled, spill the run to HDD
             */
            Run *merged = new Run(head->next, runningCount);
            RowCount nRecord = _ssd->writeNextChunk(writer, merged);
            printvv("\t\tSTATE -> Merging runs, Spill to %s %lld records \n",
                    writer->getFilename().c_str(), runningCount);
            printvv(
                "\t\tACCESS -> A write to SSD was made with size %llu bytes and latency %d ms\n",
                runningCount * Config::RECORD_SIZE, getSSDAccessTime(runningCount));
            if (nRecord != runningCount) {
                std::string msg = "ERROR: Writing run to file: nRec " + std::to_string(nRecord) +
                                  " != runningCount " + std::to_string(runningCount);
                printvv("%s\n", msg.c_str());
                throw std::runtime_error(msg);
            }
            // free memory
            delete merged;
            // reset the head
            head->next = nullptr;
            current = head;
            runningCount = 0;
        }
        flushv();
    }
    if (runningCount > 0) {
        /** write the remaining records */
        Run *merged = new Run(head->next, runningCount);
        RowCount nRecord = _ssd->writeNextChunk(writer, merged);
        printvv("\t\tSTATE -> Merged runs, Spill to %s %lld records\n",
                writer->getFilename().c_str(), runningCount);
        printvv("\t\tACCESS -> A write to SSD was made with size %llu bytes and latency %d ms\n",
                runningCount * Config::RECORD_SIZE, getSSDAccessTime(runningCount));
        flushv();
        assert(nRecord == runningCount && "ERROR: Writing run to file");
        // free memory
        delete merged;
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

    // free memory
    delete head;
    // for (auto streamer : runStreamers) { // NOTE: loserTree will delete the streamers
    //     delete streamer;
    // }

    printvv("\tMERGE_SSD_RUNS COMPLETE: Merged %d runs\n", runFiles.size());
    // print all device information
    flushvv();
    printv("\t\t\tSorted %lld records in SSD\n", nSorted);
    printStates("DEBUG: after mergeSSDRuns:");
}


// =========================================================
// -------------------- In-memory Quick Sort ---------------
// =========================================================


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
            // std::swap(records[i], records[j]);
            char *temp = records[i]->data;
            records[i]->data = records[j]->data;
            records[j]->data = temp;
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


RowCount DRAM::loadInput(RowCount nRecords) {
    // TRACE(true);
    HDD *_hdd = HDD::getInstance();
    /**
     * 1. read records from HDD to DRAM
     **/
    char *data = new char[nRecords * Config::RECORD_SIZE];
    RowCount nRecordsRead = _hdd->readRecords(data, nRecords);
    if (nRecordsRead == 0) { printvv("WARNING: no records read\n"); }
    /**
     * 2. create a linked list of records
     **/
    Record *head = new Record();
    Record *curr = head;
    char *recordsData = data;
    for (RowCount i = 0; i < nRecordsRead; i++) {
        Record *rec = new Record(recordsData);
        curr->next = rec;
        curr = rec;
        // update
        recordsData += Config::RECORD_SIZE;
    }
    _head = head->next;
    /**
     * 3. update DRAM usage
     */
    _filled += nRecordsRead;

    // print debug information
    printvv("\t\tSTATE -> Read %llu input records\n", nRecordsRead);
    printvv("\t\tACCESS -> A read from HDD was made with size %llu bytes and latency %d ms\n",
            nRecordsRead * Config::RECORD_SIZE, getHDDAccessTime(nRecordsRead));
    printv("%s\n", this->reprUsageDetails().c_str());
    flushv();
    // free memory
    delete[] data;
    delete head;
    return nRecordsRead;
}


void DRAM::genMiniRuns(RowCount nRecords, HDD *outputStorage) {
    // TRACE(true);
    printvv("\tGEN_MINIRUNS START\n");

    /**
     * 1. sort the records in cache-sized chunks and create miniruns
     */
    RowCount _cacheSize = Config::CACHE_SIZE / Config::RECORD_SIZE;
    std::vector<Run *> _miniruns;
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
        Run *run = new Run(records[0], records.size());
#if defined(_VALIDATE)
        if (run->isSorted() == false) {
            printvv("ERROR: Run is not sorted\n");
            throw std::runtime_error("Run is not sorted");
        }
#endif
        _miniruns.push_back(run);
    }
    printvv("\tSorted %lld records and generated %d miniruns\n", nRecords, _miniruns.size());
    flushv();

    /**
     * 2. setup merging state
     * -> uses the default FAN_IN FAN_OUT ratios
     * -> calculate the total input buffer size and the total output buffer size in DRAM
     * -> calculate the effective cluster size
     * -> sets the filled input and output clusters to 0
     */
    setupMergeStateForMiniruns(outputStorage->getPageSizeInRecords());
    printv("\t\t\tAfter setting up merging state in mergeMini: %s\n",
           this->reprUsageDetails().c_str());

    /**
     * 3. spill the runs that don't fit in InputClusters to SSD
     */
    RowCount totalInBufSizeDram = this->getTotalSpaceInInputClusters();
    RowCount keepNRecordsInDRAM = 0;
    size_t i = 0;
    for (; i < _miniruns.size(); i++) {
        RowCount size = _miniruns[i]->getSize();
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
            spillNRecords += _miniruns[j]->getSize();
            outputStorage->storeRun(_miniruns[j]);
            // printv("\t\t\t\tSpilled run %d to SSD\n", j);
            // flushv();
            // free memory
            delete _miniruns[j];
        }
        _miniruns.erase(_miniruns.begin() + i, _miniruns.end());

        printvv("\t\tSTATE -> %d cache-sized miniruns Spill to %s\n", j - i,
                outputStorage->getName().c_str());
        printvv("\t\tACCESS -> A write to %s was made with size %llu bytes and latency %d ms\n",
                outputStorage->getName().c_str(), spillNRecords * Config::RECORD_SIZE,
                getSSDAccessTime(spillNRecords));
    } else {
        printv("\t\t\tDEBUG: All miniruns fit in DRAM\n");
    }
    flushv();

    /**
     * 4. remaining runs fit in DRAM, merge them
     *  -
     */
    // construct loser tree
    std::vector<RunStreamer *> runStreamers;
    for (size_t i = 0; i < _miniruns.size(); i++) {
        runStreamers.push_back(new RunStreamer(StreamerType::INMEMORY_RUN, _miniruns[i]));
    }
    LoserTree loserTree;
    loserTree.constructTree(runStreamers);
    RunWriter *writer = outputStorage->getRunWriter();
    printvv("\t\tSTATE -> Merging %d cache-sized miniruns\n", _miniruns.size());
    // start merging
    Record *head = new Record();
    Record *current = head;
    RowCount nSorted = 0;
    RowCount runningCount = 0;
    while (true) {
        Record *winner = loserTree.getNext();
        if (winner == NULL) { break; }
        current->next = winner;
        current = current->next;
        nSorted++;
        runningCount++;
        if (nSorted > keepNRecordsInDRAM) { // verify the size
            printvv("ERROR: Merged run size exceeds %lld\n", keepNRecordsInDRAM);
            throw std::runtime_error("Merged run size exceeds");
        }
        if (runningCount >= _totalSpaceInOutputClusters) {
            /**
             * 4.1. when the merged run size fills the output buffer size, store the run in SSD
             */
            Run *merged = new Run(head->next, runningCount);
            RowCount nRecord = outputStorage->writeNextChunk(writer, merged);
            assert(nRecord == runningCount && "ERROR: Writing run to file");
            printvv("\t\tACCESS -> A write to %s was made with size %llu bytes and latency %d ms\n",
                    outputStorage->getName().c_str(), runningCount * Config::RECORD_SIZE,
                    outputStorage->getAccessTimeInMillis(runningCount));
            // free memory
            delete merged;
            // reset the head
            head->next = nullptr;
            current = head;
            runningCount = 0;
        }
    }
    if (runningCount > 0) {
        /* write the remaining records */
        printv("\t\t\tWriting the remaining %lld records to SSD\n", runningCount);
        Run *merged = new Run(head->next, runningCount);
#if defined(_VALIDATE)
        if (merged->isSorted() == false) {
            printvv("ERROR: Run is not sorted\n");
            throw std::runtime_error("Run is not sorted");
        }
#endif
        RowCount nRecord = outputStorage->writeNextChunk(writer, merged);
        assert(nRecord == runningCount && "ERROR: Writing remains of run to file");
        printvv("\t\tACCESS -> A write to %s was made with size %llu bytes and latency %d ms\n",
                outputStorage->getName().c_str(), runningCount * Config::RECORD_SIZE,
                outputStorage->getAccessTimeInMillis(runningCount));
        // free memory
        delete merged;
    }
    outputStorage->closeWriter(writer);
    assert(outputStorage->getTotalFilledSpaceInRecords() >= nSorted &&
           "ERROR: outputStorage filled space mismatch");

    /**
     * 5. reset the DRAM and the merge state, The DRAM should be empty now
     */
    this->reset();
    this->resetMergeState();

    // final print
    printvv("\tGEN_MINIRUNS COMPLETE: Merged %lld records and Spill to %s ------\n", nSorted,
            outputStorage->getName().c_str());
    flushvv();

    // free memory
    if (head != nullptr) {
        delete head;
        head = nullptr;
    }
    for (auto runStreamer : runStreamers) {
        delete runStreamer;
    }
    // // for (auto run : _miniruns) { //??
    // //     delete run;
    // // }
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

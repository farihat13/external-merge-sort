#include "Sort.h"
#include <chrono>

// ============================================================================
// ---------------------------------- SortPlan --------------------------------
// ============================================================================

SortPlan::SortPlan(Plan *const input) : _input(input) { TRACE(true); } // SortPlan::SortPlan

SortPlan::~SortPlan() {
    TRACE(true);
    delete _input;
} // SortPlan::~SortPlan

Iterator *SortPlan::init() const {
    TRACE(true);
    return new SortIterator(this);
} // SortPlan::init


// ============================================================================
// ------------------------------- SortIterator -------------------------------
// ============================================================================


SortIterator::SortIterator(SortPlan const *const plan)
    : _plan(plan), _input(plan->_input->init()), _consumed(0), _produced(0) {

    this->_hdd = HDD::getInstance();
    this->_ssd = SSD::getInstance();
    this->_dram = DRAM::getInstance();

    externalMergeSort();
} // SortIterator::SortIterator

SortIterator::~SortIterator() {
    TRACE(true);
    traceprintf("produced %lu of %lu rows\n", (unsigned long)(_produced),
                (unsigned long)(_consumed));
} // SortIterator::~SortIterator

bool SortIterator::next() {
    TRACE(true);
    return false; // TODO: remove

    if (_produced >= _consumed) return false;

    ++_produced;
    return true;
} // SortIterator::next

void SortIterator::getRecord(Record *r) { TRACE(true); } // SortIterator::getRecord

void SortIterator::getPage(Page *p) { TRACE(true); } // SortIterator::getPage


void SortIterator::firstPass() {
    TRACE(true);

    bool okay = _hdd->readFrom(Config::INPUT_FILE);
    if (!okay) {
        printvv("ERROR: unable to read from input file\n");
        exit(EXIT_FAILURE);
    }

    int printStatus = 1;
    _consumed = 0;
    while (true) {
        if (_consumed >= Config::NUM_RECORDS) {
            printvv("All input records read\n");
            break;
        }

        int consumedPerc = (int)(((double)_consumed) * 100.0 / Config::NUM_RECORDS);
        if (consumedPerc > (printStatus * 100.0 / 5.0)) {
            printvv("\tConsumed %d%% input. %llu out of %lld records in input\n\t", consumedPerc,
                    _consumed, Config::NUM_RECORDS);
            for (int i = 0; i < printStatus; ++i)
                printvv("==");
            printvv(">\n");
            flushvv();
            printStatus++;
        }

        /**
         * 3. if SSD is full, merge runs in SSD and spill some runs to HDD
         */
        RowCount nRecordsLeft = Config::NUM_RECORDS - _consumed;
        RowCount _ssdCurrSize = _ssd->getTotalFilledSpaceInRecords();
        RowCount nRecordsNext = std::min(nRecordsLeft, _dramCapacity);
        if (_ssdCurrSize + nRecordsNext > _ssd->getMergeFanInRecords()) {
            // spill some runs to HDD
            _ssd->mergeSSDRuns(_hdd);
        }

        /**
         * 1. Read records from input file to DRAM
         */
        PageCount nHDDPages = _dramCapacity / _hddPageSize;
        RowCount nRecordsToRead = nHDDPages == 0 ? _dramCapacity : nHDDPages * _hddPageSize;
        nRecordsToRead = std::min(nRecordsToRead, nRecordsLeft);
        RowCount nRecords = _dram->loadInput(nRecordsToRead);
        if (nRecords == 0) {
            printv("WARNING: no records read\n");
            break;
        }
        _consumed += nRecords;
        printv("\tconsumed %llu records, left %llu records in input\n", _consumed,
               Config::NUM_RECORDS - _consumed);

        /**
         * 2. Sort records in DRAM, create mini-runs, merge mini-runs, store them in SSD and reset
         * DRAM
         */
        _dram->genMiniRuns(nRecords, _ssd);
    }
    _hdd->closeRead(); // close the input file
    if (_ssd->getRunfilesCount() > 0) {
        // Merge all runs in SSD, expecting the merged run to spill to HDD
        _ssd->mergeSSDRuns(_hdd);
    }

#if defined(_VALIDATE)
    // verify the input size and consumed records is same
    printv("VALIDATE: input size %llu == consumed %llu\n", Config::NUM_RECORDS, _consumed);
    flushv();
    assert(_consumed == Config::NUM_RECORDS && "consumed records mismatch");
#endif
} // SortIterator::firstPass


void SortIterator::externalMergeSort() {
    TRACE(true);


    _hddCapacity = _hdd->getCapacityInRecords();
    _hddPageSize = _hdd->getPageSizeInRecords();
    _ssdCapacity = _ssd->getCapacityInRecords();
    _ssdPageSize = _ssd->getPageSizeInRecords();
    _dramCapacity = _dram->getCapacityInRecords();
    _dramPageSize = _dram->getPageSizeInRecords();


    /**
     * 1. First pass: read records from input file to DRAM, sort them, and spill runs to SSD and
     * HDD, and merge runs in SSD
     */
    printvv("\n========= EXTERNAL_MERGE_SORT START =========\n");
    auto start = std::chrono::steady_clock::now();
    this->firstPass();
    auto endFirstPass = std::chrono::steady_clock::now();
    auto durFirstPass = std::chrono::duration_cast<std::chrono::seconds>(endFirstPass - start);
    printvv("============= FIRST_PASS COMPLETE ===========\n");
    printvv("First_Pass Duration: %lld seconds / %lld minutes\n", durFirstPass.count(),
            durFirstPass.count() / 60);
    flushvv();

    /**
     * 2. Merge all runs in SSD and HDD
     *
     */
    int mergeIteration = 0;
    while (true) {
        int nRFilesInSSD = _ssd->getRunfilesCount();
        int nRFilesInHDD = _hdd->getRunfilesCount();
        printvv("\tMERGE_ITR %d: %d runfiles in SSD, %d runfiles in HDD\n", mergeIteration,
                nRFilesInSSD, nRFilesInHDD);
        flushvv();
        auto startMerge = std::chrono::steady_clock::now();
        if (nRFilesInSSD < 0 || nRFilesInHDD < 0) {
            printvv("ERROR: invalid runs in SSD or HDD\n");
            throw std::runtime_error("invalid runs in SSD or HDD");
            break;
        }
        if (nRFilesInHDD == 0) { // no runfiles in HDD
            if (nRFilesInSSD < 1) {
                printvv("ERROR: no runs to merge\n");
                break;
            } else if (nRFilesInSSD == 1) {
                printvv("SUCCESS: all runs merged\n");
                // rename the runfile to output file
                std::string src = _ssd->getRunfile(0);
                std::string dest = Config::OUTPUT_FILE;
                rename(src.c_str(), dest.c_str());

                break;
            } // else: merge runs in SSD
            _ssd->mergeSSDRuns(_hdd);
        } else { // some runfiles in HDD
            if (nRFilesInHDD == 1 && nRFilesInSSD == 0) {
                /** all runs merged, because there is only one run in HDD, and no runs in SSD */
                printvv("SUCCESS: all runs merged\n");
                // rename the runfile to output file
                std::string src = _hdd->getRunfile(0);
                std::string dest = Config::OUTPUT_FILE;
                rename(src.c_str(), dest.c_str());

                break;
            } else {
                _hdd->mergeHDDRuns();
            }
        }
        auto endMerge = std::chrono::steady_clock::now();
        auto durMerge = std::chrono::duration_cast<std::chrono::seconds>(endMerge - startMerge);
        printvv("\tMERGE_ITR %d COMPLETE: Duration %lld seconds / %lld minutes\n", mergeIteration,
                durMerge.count(), durMerge.count() / 60);
        ++mergeIteration;
    }
    auto endMerge = std::chrono::steady_clock::now();
    auto durMerge = std::chrono::duration_cast<std::chrono::seconds>(endMerge - endFirstPass);
    auto durTotal = std::chrono::duration_cast<std::chrono::seconds>(endMerge - start);
    printvv("======== EXTERNAL_MERGE_SORT COMPLETE =========\n");
    printvv("External_Merge_Sort Total Duration %lld seconds / %lld minutes\n", durTotal.count(),
            durTotal.count() / 60);
    printvv("Removed %lld duplicate records out of %lld duplicates\n",
            Config::NUM_DUPLICATES_REMOVED, Config::NUM_DUPLICATES);
    printvv("===============================================\n");
    flushvv();
} // SortIterator::externalMergeSort

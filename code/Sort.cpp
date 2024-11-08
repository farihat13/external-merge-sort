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

    // Skip sorting if there are no records
    if (Config::NUM_RECORDS == 0) {
        printvv("No records to sort\n");
        return;
    }

    // Skip sorting if there is only one record
    if (Config::NUM_RECORDS == 1) {
        printvv("Only one record to sort\n");
        return;
    }

    // Verify input file exists
    bool okay = _hdd->readFrom(Config::INPUT_FILE);
    if (!okay) {
        printvv("ERROR: unable to read from input file\n");
        exit(EXIT_FAILURE);
    }

    int printStatus = 1;
    _consumed = 0;
    while (true) {
        // Check if all input records are read
        if (_consumed >= Config::NUM_RECORDS) {
            printvv("All input records read\n");
            break;
        }

        // Print status
        double consumedPerc = ((double)_consumed) * 100.0 / Config::NUM_RECORDS;
        if (consumedPerc > printStatus * 1.0) {
            printvv("\tConsumed %.1lf%% input. %llu out of %lld records.\n\t", consumedPerc,
                    _consumed, Config::NUM_RECORDS);
            prettyPrintPercentage(consumedPerc);
            printStatus++;
        }


        // If the next DRAM load will exceed SSD capacity, merge runs in SSD
        // This will spill the merged run to HDD and free up space in SSD
        RowCount nRecordsLeft = Config::NUM_RECORDS - _consumed;
        RowCount _ssdCurrSize = _ssd->getTotalFilledSpaceInRecords();
        RowCount nRecordsNext = std::min(nRecordsLeft, _dramCapacity);
        if (_ssdCurrSize + nRecordsNext > _ssd->getMergeFanInRecords()) {
            // spill some runs to HDD
            _ssd->mergeSSDRuns(_hdd);
        }

        // Read records from input file to DRAM
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

        // Sort records in DRAM
        // - Create mini-runs using quicksort,
        // - Spill some mini-runs to SSD to free up space for output buffer in DRAM
        // - Merge mini-runs, store the merged run in SSD and reset DRAM
        _dram->genMiniRuns(nRecords, _ssd);
    }

    // Close the input file
    _hdd->closeRead();


    if (_ssd->getRunfilesCount() > 1) {
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


    // First pass
    // - Read records from input file to DRAM, sort and merge them, and spill runs to SSD and HDD
    // - When SSD is full, merge runs in SSD and spill the merged run to HDD
    // - Repeat until all input records are read
    printvv("\n========= EXTERNAL_MERGE_SORT START =========\n");
    auto start = std::chrono::steady_clock::now();
    this->firstPass();
    auto endFirstPass = std::chrono::steady_clock::now();
    auto durFirstPass = std::chrono::duration_cast<std::chrono::seconds>(endFirstPass - start);
    printvv("============= FIRST_PASS COMPLETE ===========\n");
    printvv("First_Pass Duration: %lld seconds / %lld minutes\n", durFirstPass.count(),
            durFirstPass.count() / 60);
    flushvv();


    // Merge all runs in SSD and HDD
    // - At this point, all runs are in SSD and HDD
    // - Merge runs in SSD and HDD until only one run is left
    int mergeIteration = 0;
    while (true) {
        // Check if all records are merged
        if (Config::NUM_RECORDS == 0) {
            printvv("SUCCESS: No records to merge\n");
            break;
        }
        if (Config::NUM_RECORDS == 1) {
            printvv("SUCCESS: Only one record to merge\n");
            // copy the input file to output file
            std::string src = Config::INPUT_FILE;
            std::string dest = Config::OUTPUT_FILE;
            std::ifstream srcFile(src, std::ios::binary);
            std::ofstream destFile(dest, std::ios::binary);
            destFile << srcFile.rdbuf();
            srcFile.close();
            destFile.close();
            break;
        }

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

        if (nRFilesInHDD == 0) {
            // No runfiles in HDD
            if (nRFilesInSSD < 1) {
                printvv("ERROR: no runs to merge\n");
                break;
            } else if (nRFilesInSSD == 1) {
                // Only one runfile in SSD, this is the final run, Rename it to output file
                printvv("SUCCESS: all runs merged\n");
                std::string src = _ssd->getRunfile(0);
                std::string dest = Config::OUTPUT_FILE;
                rename(src.c_str(), dest.c_str());
                break;
            }

            // More than one runfile in SSD, merge them
            _ssd->mergeSSDRuns(_hdd);

        } else {
            // There are runfiles in HDD
            if (nRFilesInHDD == 1 && nRFilesInSSD == 0) {
                // Only one runfile in HDD, this is the final run, Rename it to output file
                printvv("SUCCESS: all runs merged\n");
                std::string src = _hdd->getRunfile(0);
                std::string dest = Config::OUTPUT_FILE;
                rename(src.c_str(), dest.c_str());
                break;
            }

            // Merge runs in SSD and HDD
            // - this will merge runs from SSD and HDD together with the help of RunStreamer
            _hdd->mergeHDDRuns();
        }
        auto endMerge = std::chrono::steady_clock::now();
        auto durMerge = std::chrono::duration_cast<std::chrono::seconds>(endMerge - startMerge);
        printvv("\tMERGE_ITR %d COMPLETE: Duration %lld seconds / %lld minutes\n", mergeIteration,
                durMerge.count(), durMerge.count() / 60);
        flushvv();

        // Increment merge iteration
        ++mergeIteration;
    }
    auto endMerge = std::chrono::steady_clock::now();
    auto durMerge = std::chrono::duration_cast<std::chrono::seconds>(endMerge - endFirstPass);
    auto durTotal = std::chrono::duration_cast<std::chrono::seconds>(endMerge - start);

    // End of external merge sort, print stats
    printvv("======== EXTERNAL_MERGE_SORT COMPLETE =========\n");
    printvv("External_Merge_Sort Total Duration %lld seconds / %lld minutes\n", durTotal.count(),
            durTotal.count() / 60);
    printvv("Removed %lld duplicate records out of %lld duplicates\n",
            Config::NUM_DUPLICATES_REMOVED, Config::NUM_DUPLICATES);
    printvv("SSD Access Count: %lld\n", Config::SSD_COUNT);
    printvv("HDD Access Count: %lld\n", Config::HDD_COUNT);
    printvv("===============================================\n");
    flushvv();
} // SortIterator::externalMergeSort

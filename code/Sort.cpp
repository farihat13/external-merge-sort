#include "Sort.h"


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

    if (_produced >= _consumed)
        return false;

    ++_produced;
    return true;
} // SortIterator::next

void SortIterator::getRecord(Record *r) { TRACE(true); } // SortIterator::getRecord

void SortIterator::getPage(Page *p) { TRACE(true); } // SortIterator::getPage


RowCount SortIterator::loadInputToDRAM() {
    TRACE(true);
    // read records from HDD to DRAM
    PageCount nHDDPages = _dramCapacity / _hddPageSize;
    RowCount nRecordsToRead = nHDDPages * _hddPageSize;
    char *records = _hdd->readRecords(&nRecordsToRead);
    if (records == NULL || nRecordsToRead == 0) {
        printvv("WARNING: no records read\n");
    }
    // loading a hdd page to dram
    _dram->loadRecordsToDRAM(records, nRecordsToRead);
    // print debug information
    printv("STATE -> %llu input records read from HDD to DRAM\n", nRecordsToRead);
    printv("ACCESS -> A read from HDD to RAM was made with size %llu bytes and latency %d ms\n",
           nRecordsToRead * Config::RECORD_SIZE, _hdd->getAccessTimeInMillis(nRecordsToRead));
    printv("DEBUG: %llu Disk pages / %llu records read from HDD to RAM\n", nHDDPages,
           nRecordsToRead);
    flushv();

    return nRecordsToRead;
}

void SortIterator::firstPass() {
    TRACE(true);

    bool okay = _hdd->readFrom(Config::INPUT_FILE);
    if (!okay) {
        printvv("ERROR: unable to read from input file\n");
        exit(EXIT_FAILURE);
    }

    _consumed = 0;
    while (true) {
        if (_consumed >= Config::NUM_RECORDS) {
            printvv("INFO: all records read\n");
            break;
        }


        // 3. Spill some sorted runs to HDD
        RowCount nRecordsLeft = Config::NUM_RECORDS - _consumed;
        RowCount _ssdCurrSize = _ssd->getTotalFilledSpaceInRecords();
        RowCount nRecordsNext = std::min(nRecordsLeft, _dramCapacity);
        // if (_ssdCurrSize + nRecordsNext > _ssdCapacity) { // TODO:
        if (_ssdCurrSize + nRecordsNext > _ssd->getMergeFanInRecords()) {
            // spill some runs to HDD
            _ssd->mergeRuns(_hdd);
        }

        // 1. Read records from input file to DRAM
        RowCount nRecords = this->loadInputToDRAM();
        if (nRecords == 0) {
            printvv("WARNING: no records read\n");
            break;
        }
        _consumed += nRecords;
        printvv("DEBUG: consumed %llu out of %llu records in input, input file position %llu\n",
                _consumed, Config::NUM_RECORDS - _consumed,
                _hdd->getReadPosition() / Config::RECORD_SIZE);
        printv("after loading input: %s\n",
               _dram->reprUsageDetails().c_str()); // print dram state

        // 2. Sort records in DRAM
        _dram->genMiniRuns(nRecords); // this should sort records in dram and create miniruns
        _dram->mergeMiniRuns(_ssd);   // this should spill runs to SSD and reset dram
        printv("%s\n", _ssd->reprUsageDetails().c_str());  // print ssd state
        printv("%s\n", _dram->reprUsageDetails().c_str()); // print dram state
    }
    _hdd->closeRead();

#if defined(_VALIDATE)
    // verify the input size and consumed records is same
    printvv("VALIDATE: input size %llu == consumed %llu\n", Config::NUM_RECORDS, _consumed);
    flushv();
    assert(_consumed == Config::NUM_RECORDS && "consumed records mismatch");
#endif
}


void SortIterator::externalMergeSort() {
    TRACE(true);

    _hddCapacity = _hdd->getCapacityInRecords();
    _hddPageSize = _hdd->getPageSizeInRecords();
    _ssdCapacity = _ssd->getCapacityInRecords();
    _ssdPageSize = _ssd->getPageSizeInRecords();
    _dramCapacity = _dram->getCapacityInRecords();
    _dramPageSize = _dram->getPageSizeInRecords();

    // TODO:
    // 1. Create initial sorted runs

    this->firstPass();
    // 2. Store runs on disk
    // 3. Initialize merge process
} // SortIterator::externalMergeSort

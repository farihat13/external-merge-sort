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
    RowCount nRecords = 0; // number of records read
    PageCount nPages = 0;  // number of pages read

    while (nRecords + _hddPageSize <= _dramCapacity) {
        RowCount toRead = _hddPageSize;
        char *records = _hdd->readRecords(&toRead);
        if (records == NULL || toRead == 0) {
            printvv("WARNING: no records read\n");
            break;
        }
        nRecords += toRead;
        nPages++;
        // loading a hdd page to dram
        _dram->loadRecordsToDRAM(records, toRead);
    }
    printv("STATE -> %llu input records read from HDD to DRAM\n", nRecords);
    printv("ACCESS -> A read from HDD to RAM was made with size %llu bytes and latency %d ms\n",
           nRecords * Config::RECORD_SIZE, _hdd->getAccessTimeInMillis(nRecords));
    printv("DEBUG: %llu Disk pages / %llu records read from HDD to RAM\n", nPages, nRecords);
    flushv();
    return nRecords;
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
        RowCount _ssdCurrSize = _ssd->getRunManager()->getCurrSizeInRecords();
        RowCount nRecordsNext = std::min(nRecordsLeft, _dramCapacity);
        // if (_ssdCurrSize + nRecordsNext > _ssdCapacity) { // TODO:
        if (_ssdCurrSize + nRecordsNext > _ssd->getMergeFanInRecords()) {
            // spill some runs to HDD
            _ssd->spillRunsToHDD(_hdd);
        }

        // 1. Read records from input file to DRAM
        _dram->resetRecords();
        RowCount nRecords = this->loadInputToDRAM();
        if (nRecords == 0) {
            printvv("WARNING: no records read\n");
            break;
        }
        _consumed += nRecords;
        printv("DEBUG: consumed %llu records, current file position %llu\n", _consumed,
               _hdd->getReadPosition() / Config::RECORD_SIZE);

        // 2. Sort records in DRAM
        _dram->genMiniRuns(nRecords);
        _dram->mergeMiniRuns(_ssd->getRunManager());
        printv("DEBUG: %s\n", _ssd->getRunManager()->repr());
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

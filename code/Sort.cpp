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


void SortIterator::externalMergeSort() {
    TRACE(true);

    // TODO:
    // 1. Create initial sorted runs
    // 2. Store runs on disk
    // 3. Initialize merge process

    RowCount hddCapacity = _hdd->getCapacityInRecords();
    RowCount hddPageSize = _hdd->getPageSizeInRecords();
    RowCount dramCapacity = _dram->getCapacityInRecords();
    RowCount dramPageSize = _dram->getPageSizeInRecords();

    while (_input->next()) {

        /*
         * Read records from input file to DRAM
         */
        RowCount nRead = 0; // number of records read
        Page *head = NULL;
        while (nRead + hddPageSize <= dramCapacity && _input->next()) {
            Page *p = new Page(hddPageSize);
            _input->getPage(p);
            if (p->sizeInRecords() == 0) {
                delete p;
                printv("EOF reached\n");
                exit(0);
                break;
            }
            nRead += p->sizeInRecords();
            if (head == NULL) {
                head = p;
            } else {
                head->addNextPage(p);
            }
        }
        _consumed += nRead;
        printv("Read %lu rows from HDD to DRAM\n", (unsigned long)(nRead));
        fflush(stdout);

        /*
         * TODO: Sort records in DRAM
         */
    }
    delete _input;

    traceprintf("consumed %llu rows (%s Bytes)\n", _consumed,
                formatNum((uint64_t)_consumed * Config::RECORD_SIZE).c_str());
} // SortIterator::externalMergeSort

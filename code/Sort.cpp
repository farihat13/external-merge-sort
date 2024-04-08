#include "Sort.h"
#include "config.h"

SortPlan::SortPlan(Plan *const input) : _input(input) {
    TRACE(true);
} // SortPlan::SortPlan

SortPlan::~SortPlan() {
    TRACE(true);
    delete _input;
} // SortPlan::~SortPlan

Iterator *SortPlan::init() const {
    TRACE(true);
    return new SortIterator(this);
} // SortPlan::init


SortIterator::SortIterator(SortPlan const *const plan)
    : _plan(plan), _input(plan->_input->init()), _consumed(0), _produced(0) {
    TRACE(true);

    // TODO:
    // 1. Create initial sorted runs
    // 2. Store runs on disk
    // 3. Initialize merge process

    char *s = new char[Config::RECORD_SIZE];
    Record r(s);
    while (_input->next()) {
        _input->getRecord(&r);
        ++_consumed;
        Logger::writef("READ %s\n", r._data);
        // Logger::writef("consumed %lu rows. key: %c\n",
        //                (unsigned long)(_consumed), r._data[0]);
    }
    delete _input;

    traceprintf("consumed %lu rows\n", (unsigned long)(_consumed));

} // SortIterator::SortIterator

SortIterator::~SortIterator() {
    TRACE(true);

    traceprintf("produced %lu of %lu rows\n", (unsigned long)(_produced),
                (unsigned long)(_consumed));
} // SortIterator::~SortIterator

bool SortIterator::next() {
    TRACE(true);

    if (_produced >= _consumed)
        return false;

    ++_produced;
    return true;
} // SortIterator::next

void SortIterator::getRecord(Record *r) {
    TRACE(true);
} // SortIterator::getRecord

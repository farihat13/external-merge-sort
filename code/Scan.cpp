#include "Scan.h"
#include <string>
#include <cstdlib>
#include <ctime>


ScanPlan::ScanPlan(RowCount const count) : _count(count) {
    TRACE(true);
} // ScanPlan::ScanPlan

ScanPlan::~ScanPlan() { TRACE(true); } // ScanPlan::~ScanPlan

Iterator *ScanPlan::init() const {
    TRACE(true);
    return new ScanIterator(this);
} // ScanPlan::init


ScanIterator::ScanIterator(ScanPlan const *const plan)
    : _plan(plan), _count(0) {
    srand(time(0));
    TRACE(true);
} // ScanIterator::ScanIterator

void ScanIterator::gen_a_record(char *s, const int len) {
    static const char alphanum[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
}

ScanIterator::~ScanIterator() {
    TRACE(true);
    traceprintf("produced %lu of %lu rows\n", (unsigned long)(_count),
                (unsigned long)(_plan->_count));
} // ScanIterator::~ScanIterator

bool ScanIterator::next() {
    TRACE(true);

    if (_count >= _plan->_count)
        return false;

    ++_count;
    return true;
} // ScanIterator::next

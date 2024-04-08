#include "Scan.h"
#include "config.h"
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
    TRACE(true);
    srand(time(0));
    std::ofstream input_file(Config::INPUT_FILE, std::ios::binary);
    char *s = new char[Config::RECORD_SIZE];
    for (RowCount i = 0; i < plan->_count; i++) {
        gen_a_record(s, Config::RECORD_SIZE);
        input_file.write(s, Config::RECORD_SIZE);
    }
    input_file.close();
    traceprintf("generated %lu records\n", (unsigned long)(plan->_count));
    this->file.open(Config::INPUT_FILE, std::ios::binary);
} // ScanIterator::ScanIterator

void ScanIterator::gen_a_record(char *s, const int len) {
    static const char alphanum[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
} // ScanIterator::gen_a_record

ScanIterator::~ScanIterator() {
    TRACE(true);
    this->file.close();
    traceprintf("produced %lu of %lu rows\n", (unsigned long)(_count),
                (unsigned long)(_plan->_count));
} // ScanIterator::~ScanIterator

bool ScanIterator::next() {
    TRACE(true);
    if (_count >= _plan->_count)
        return false;

    _count++;
    return true;
} // ScanIterator::next

void ScanIterator::getRecord(Record *r) {
    TRACE(true);
    this->file.read(r->_data, Config::RECORD_SIZE);
    ++_count;
}

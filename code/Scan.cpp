#include "Scan.h"

#include <cstdlib>
#include <ctime>
#include <string>


// =========================================================
// ------------------------- ScanPlan ----------------------
// =========================================================


ScanPlan::ScanPlan(RowCount const count, std::string const filename)
    : _count(count), _filename(filename) {
    TRACE(true);
} // ScanPlan::ScanPlan

ScanPlan::~ScanPlan() { TRACE(true); } // ScanPlan::~ScanPlan


// =========================================================
// ------------------------- ScanIterator ------------------
// =========================================================

/**
 * @ a utility function to generate a random record
 */
void gen_a_record(char *s, const int len) {
    static const char alphanum[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
} // ScanIterator::gen_a_record


Iterator *ScanPlan::init() const {
    TRACE(true);
    return new ScanIterator(this);
} // ScanPlan::init


// constructor for scanning the input file
ScanIterator::ScanIterator(ScanPlan const *const plan) : _plan(plan), _count(0) {
    TRACE(true);

    // this->_hdd = HDD::getInstance();

    // skip if the input file already exists
    if (!std::ifstream(plan->_filename.c_str())) {
        // seed random number generator if debug is not defined
        srand(time(0));
#if defined(_DEBUG)
        // seed is fixed for reproducibility
        srand(100);
#endif

        std::ofstream input_file(_plan->_filename, std::ios::binary);
        char *record = new char[Config::RECORD_SIZE];
        for (RowCount i = 0; i < plan->_count; i++) {
            gen_a_record(record, Config::RECORD_SIZE);
            record[Config::RECORD_SIZE - 1] = '\n'; // TODO: remove later
            input_file.write(record, Config::RECORD_SIZE);
        }
        input_file.close();
        traceprintf("generated %lu records\n", (unsigned long)(plan->_count));
        delete[] record;

#if defined(_DEBUG)
        // verify the file size
        std::ifstream inputfile(_plan->_filename, std::ios::binary);
        inputfile.seekg(0, std::ios::end);
        long long file_size = inputfile.tellg();
        long long expected = plan->_count * Config::RECORD_SIZE;
        if (file_size != expected) {
            traceprintf("file size %lld != expected %lld\n", file_size, expected);
        }
#endif
    }

    this->_file.open(_plan->_filename, std::ios::binary);
    this->_file.seekg(0, std::ios::beg);

} // ScanIterator::ScanIterator


ScanIterator::~ScanIterator() {
    TRACE(true);
    this->_file.close();
    traceprintf("produced %lu of %lu rows\n", (unsigned long)(_count),
                (unsigned long)(_plan->_count));
} // ScanIterator::~ScanIterator

bool ScanIterator::next() {
    TRACE(true);
    if (_count >= _plan->_count)
        return false;
    return true;
} // ScanIterator::next

void ScanIterator::getRecord(Record *r) { TRACE(true); }

void ScanIterator::getPage(Page *p) {
    TRACE(true);
    p->read(this->_file);
    traceprintf("read %llu records\n", p->sizeInRecords());
    this->_currpage = p;
    _count += p->sizeInRecords();
} // ScanIterator::getPage

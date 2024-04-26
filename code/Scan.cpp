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


RowCount genInput(const std::string &filename, const RowCount count) {
    // seed random number generator if debug is not defined
    srand(time(0));
#if defined(_DEBUG)
    // seed is fixed for reproducibility
    // srand(100);
#endif
    traceprintf("generating input file '%s'\n", filename.c_str());
    std::string tmpfilename = filename + ".tmp";
    std::ofstream input_file(tmpfilename, std::ios::binary);
    char *record = new char[Config::RECORD_SIZE];
    for (RowCount i = 0; i < count; i++) {
        gen_a_record(record, Config::RECORD_SIZE);
        record[Config::RECORD_SIZE - 1] = '\n'; // TODO: remove later
        input_file.write(record, Config::RECORD_SIZE);
    }
    input_file.close();
    // rename the file
    rename(tmpfilename.c_str(), filename.c_str());
    // cleanup
    delete[] record;
    return count;
}

RowCount genInputBatch(const std::string &filename, const RowCount count) {
    srand(time(0));
#if defined(_DEBUG)
    // seed is fixed for reproducibility
    // srand(100);
#endif
    traceprintf("generating input file '%s'\n", filename.c_str());
    std::string tmpfilename = filename + ".tmp";
    std::ofstream input_file(tmpfilename, std::ios::binary | std::ios::trunc);

    /**
     * generate records in batches
     */
    // calculate batch size and number of batches
    RowCount batchSize = 4096;
    batchSize = std::min(batchSize, count);
    if (batchSize % 2 != 0) { batchSize--; }
    RowCount nBatches = batchSize == 0 ? 0 : (count / batchSize);
    // allocate buffer for batch records
    char *buffer = new char[Config::RECORD_SIZE * batchSize];
    RowCount n = 0;
    RowCount dup = 0;
    for (RowCount i = 0; i < nBatches; i++) {
        char *record = buffer;
#if defined(_DUP_GEN)
        for (RowCount j = 0; j < batchSize / 2; j++) {
            gen_a_record(record, Config::RECORD_SIZE);
            record[Config::RECORD_SIZE - 1] = '\n'; // TODO: remove later
            n++;
            record += Config::RECORD_SIZE;
        }
        char *duplicate = buffer;
        for (RowCount j = 0; j < batchSize / 2; j++) {
            memcpy(record, duplicate, Config::RECORD_SIZE);
            n++;
            record += Config::RECORD_SIZE;
            duplicate += Config::RECORD_SIZE;
            dup++;
        }
#else
        for (RowCount j = 0; j < batchSize; j++) {
            gen_a_record(record, Config::RECORD_SIZE);
            record[Config::RECORD_SIZE - 1] = '\n'; // TODO: remove later
            n++;
            record += Config::RECORD_SIZE;
        }
#endif
        input_file.write(buffer, Config::RECORD_SIZE * batchSize);
        // printv("%lld\n", n);
    }
    batchSize = count % batchSize;
    if (batchSize > 0) {
        char *record = buffer;
        for (RowCount j = 0; j < batchSize; j++) {
            gen_a_record(record, Config::RECORD_SIZE);
            record[Config::RECORD_SIZE - 1] = '\n'; // TODO: remove later
            n++;
            record += Config::RECORD_SIZE;
        }
        input_file.write(buffer, Config::RECORD_SIZE * batchSize);
        // printv("%lld\n", n);
    }
    traceprintf("generated %lu records (%lu of them are duplicate)\n", n, dup);
    input_file.close();
    // rename the file
    rename(tmpfilename.c_str(), filename.c_str());

    // cleanup
    delete[] buffer;
    // return
    return n;
}


Iterator *ScanPlan::init() const {
    TRACE(true);
    return new ScanIterator(this);
} // ScanPlan::init


// constructor for scanning the input file
ScanIterator::ScanIterator(ScanPlan const *const plan) : _plan(plan), _count(0) {
    TRACE(true);
    // skip if the input file already exists
    if (!std::ifstream(plan->_filename.c_str())) {
        // RowCount n = genInput(plan->_filename, plan->_count);
        RowCount n = genInputBatch(plan->_filename, plan->_count);
        if (n != plan->_count) {
            printv("ERROR: generated %lld records instead of %lld\n", n, plan->_count);
            exit(1);
        }
    }
    // NOTE: did not update HDD usage value since its capacity is infinite
    traceprintf("\tinput file %s, size %s\n", _plan->_filename.c_str(),
                getSizeDetails(_plan->_count * Config::RECORD_SIZE).c_str());
    printv("\tinput file has %llu records\n", _plan->_count);

} // ScanIterator::ScanIterator


ScanIterator::~ScanIterator() {
    TRACE(true);
    // this->_file.close();
    // traceprintf("produced %lu of %lu rows\n", (unsigned long)(_count),
    // (unsigned long)(_plan->_count));
} // ScanIterator::~ScanIterator

bool ScanIterator::next() {
    TRACE(true);
    return false;
    // if (this->_file.eof()) {
    //     traceprintf("reached end of file\n");
    //     traceprintf("read %lu of %lu rows\n", (unsigned long)(_count),
    //                 (unsigned long)(_plan->_count));
    //     return false;
    // }
    // if (_count >= _plan->_count) {
    //     traceprintf("read all input rows. %lu of %lu rows\n", (unsigned long)(_count),
    //                 (unsigned long)(_plan->_count));
    //     return false;
    // }
    // return true;
} // ScanIterator::next

void ScanIterator::getRecord(Record *r) { TRACE(true); }

void ScanIterator::getPage(Page *p) {
    // TRACE(true);
    // p->read(this->_file);
    // traceprintf("read %llu records\n", p->sizeInRecords());
    // this->_currpage = p;
    // _count += p->sizeInRecords();
} // ScanIterator::getPage

#include "Scan.h"

#include <chrono>
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
    srand(100);
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
    // free memory
    delete[] record;
    return count;
}


/**
 * Generate input file with duplicate records, in batch mode
 */
RowCount genInputBatch(const std::string &filename, const RowCount count) {
    srand(time(0));
#if defined(_DEBUG)
    // seed is fixed for reproducibility
    srand(100);
#endif
    printv("generating input file '%s'\n", filename.c_str());
    std::string tmpfilename = filename + ".tmp";
    std::ofstream input_file(tmpfilename, std::ios::binary | std::ios::trunc);

    // Calculate batch size and number of batches
    RowCount batchSize = 4096;
    if (batchSize * Config::RECORD_SIZE > 1024*1024*1024) {
        batchSize = 1024*1024*1024 / Config::RECORD_SIZE;
        batchSize = std::min(batchSize, (RowCount) 2);
    }
    batchSize = std::min(batchSize, count);
    if (batchSize % 2 != 0) { batchSize--; }
    RowCount nBatches = batchSize == 0 ? 0 : (count / batchSize);
    // Allocate buffer for batch records
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
            // Duplicate the record
            memcpy(record, record - Config::RECORD_SIZE, Config::RECORD_SIZE);
            dup++;
            n++;
            record += Config::RECORD_SIZE;
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
    }
    // free memory
    delete[] buffer;
    batchSize = count - n;
    buffer = new char[Config::RECORD_SIZE * batchSize];
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
    input_file.close();
    // rename the file
    rename(tmpfilename.c_str(), filename.c_str());
    Config::NUM_DUPLICATES = dup;
    printvv("Generated %lu records (%lu of them are duplicate) in %s\n", n, dup, filename.c_str());

    // free memory
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


    if (!std::ifstream(plan->_filename.c_str())) {
        // Generate input file
        printvv("========== INPUT_GEN START ========\n");
        flushvv();
        auto start = std::chrono::steady_clock::now();
        RowCount n = genInputBatch(plan->_filename, plan->_count);
        auto end = std::chrono::steady_clock::now();
        auto dur = std::chrono::duration_cast<std::chrono::seconds>(end - start);
        printvv("======= INPUT_GEN COMPLETE ========\n");
        printvv("Input_Gen Duration %lld seconds / %lld minutes\n", dur.count(), dur.count() / 60);
        flushvv();

        // Verify the number of records generated
        if (n != plan->_count) {
            printvv("ERROR: generated %lld records instead of %lld\n", n, plan->_count);
            exit(1);
        }
    } else {
        // Skip if the input file already exists
        printvv("input file '%s' already exists\n", plan->_filename.c_str());
        flushvv();
    }

    // NOTE: did not update HDD usage value since its capacity is infinite
    printv("\tinput file %s, size %s\n", _plan->_filename.c_str(),
           getSizeDetails(_plan->_count * Config::RECORD_SIZE).c_str());
    printv("\tinput file has %llu records\n", _plan->_count);

} // ScanIterator::ScanIterator


ScanIterator::~ScanIterator() {
    // TRACE(true);
} // ScanIterator::~ScanIterator

bool ScanIterator::next() {
    // TRACE(true);
    return false;
} // ScanIterator::next

void ScanIterator::getRecord(Record *r) {
    // TRACE(true);
}

void ScanIterator::getPage(Page *p) {
    // TRACE(true);
} // ScanIterator::getPage

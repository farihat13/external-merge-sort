#include "Record.h"


// =========================================================
// ------------------------- Record -------------------------
// =========================================================


// to string
char *Record::reprKey() {
    char *key = new char[Config::RECORD_KEY_SIZE + 1];
    std::strncpy(key, data, Config::RECORD_KEY_SIZE);
    key[Config::RECORD_KEY_SIZE] = '\0';
    return key;
}

char *Record::repr() {
    char *key = new char[Config::RECORD_SIZE + 1];
    std::strncpy(key, data, Config::RECORD_SIZE);
    key[Config::RECORD_SIZE] = '\0';
    return key;
}

bool Record::isValid() {
    if (data == nullptr) return false;
    if (this == getMaxRecord()) return true;
    for (int i = 0; i < Config::RECORD_SIZE; i++) {
        if (!isalnum(data[i])) return false;
    }
    return true;
}

Record *MAX_RECORD = nullptr;
Record *getMaxRecord() {
    if (MAX_RECORD == nullptr) {
        MAX_RECORD = new Record();
        for (int i = 0; i < Config::RECORD_SIZE; i++) {
            MAX_RECORD->data[i] = '~';
        }
    }
    return MAX_RECORD;
}
bool isRecordMax(Record *r) {
    char a = getMaxRecord()->data[0];
    char b = r->data[0];
    return a == b;
}

// =========================================================
// -------------------------- Page -------------------------
// =========================================================


int Page::addRecord(Record *rec) {
    if (records.size() >= capacity) {
        fprintf(stderr, "Error: Page addRecord exceeds page size\n");
        return -1;
    }
    if (records.size() > 0) {
        Record *lastRecord = records.back();
        lastRecord->next = rec;
    } else {
        // first record
        rec->next = nullptr;
    }
    records.push_back(rec);
    return 0;
}


int Page::read(std::ifstream &is) {
    if (is.eof()) {
        // traceprintf("EOF reached\n");
        return 0;
    }
    // 1.1 read page.capacityInRecords() records
    int nBytesRead = this->getCapacityInRecords() * Config::RECORD_SIZE;
    char *recData = new char[nBytesRead];
    is.read(recData, nBytesRead);
    nBytesRead = is.gcount();
    // 1.2 if no bytes read, delete record and break
    if (nBytesRead == 0) {
        delete[] recData;
        return 0;
    }
    // 1.3 if bytes read is not equal to record size, throw error
    if (nBytesRead % Config::RECORD_SIZE != 0) {
        throw std::runtime_error("Error: Read " + std::to_string(nBytesRead) + " bytes, expected " +
                                 std::to_string(Config::RECORD_SIZE));
    }
    // 1.4 add records to page
    for (int i = 0; i < nBytesRead / Config::RECORD_SIZE; i++) {
        Record *rec = new Record(recData + i * Config::RECORD_SIZE);
        this->addRecord(rec);
    }

    return this->getSizeInRecords();
}

int Page::write(std::ofstream &os) {
    for (auto rec : records) {
        os.write(rec->data, Config::RECORD_SIZE);
        if (!os) { throw std::runtime_error("Error: Writing to file"); }
    }
    return this->getSizeInRecords();
}

bool Page::isSorted() const {
    Record *rec = records[0];
    while (rec->next != nullptr) {
        if (rec->next->data < rec->data) { return false; }
        rec = rec->next;
    }
    return true;
}


bool Page::isValid() const {
    // 1. check if capacity exceeds size of records
    if (capacity < records.size()) { return false; }
    // 3. check if all records are valid;
    // // NOTE: uncomment for extra check
    // for (auto rec : records) {
    //     if (!rec->isValid()) {
    //         return false;
    //     }
    // }
    // 4. check if all records are chained
    // // NOTE: uncomment for extra check
    // for (int i = 0; i < records.size() - 1; i++) {
    //     if (records[i]->next != records[i + 1]) {
    //         return false;
    //     }
    // }
    return true;
}

// =========================================================
// ------------------------- RunReader ---------------------
// =========================================================


Page *RunReader::readNextPage() {
    Page *page = new Page(PAGE_SIZE_IN_RECORDS);
    // 1. read a page
    RowCount nRecords = page->read(_is);
    if (nRecords == 0) { return nullptr; }
    // 2. if page is not valid, throw error
    if (!page->isValid()) { throw std::runtime_error("Error: Page is not valid"); }
    // 3. check if all records are sorted
    if (!page->isSorted()) { throw std::runtime_error("Error: Page is not sorted"); }
    _nRecordsRead += nRecords;
    return page;
}


// =========================================================
// ----------------------- RunWriter -----------------------
// =========================================================


RowCount RunWriter::writeNextPage(Page *page) {
    // 1. check validity of a page
    if (!page->isValid()) { throw std::runtime_error("Error: Page is not valid"); }
    // 2. check if all records are sorted
    if (!page->isSorted()) { throw std::runtime_error("Error: Page is not sorted"); }
    // 3. write a page
    RowCount nRecords = page->write(_os);
    if (nRecords != page->getSizeInRecords()) {
        throw std::runtime_error("Error: Writing " + std::to_string(nRecords) +
                                 " records, expected " + std::to_string(page->getSizeInRecords()));
    }
    currSize += nRecords;
    return nRecords;
}

RowCount RunWriter::writeFromFile(std::string writeFromFilename, RowCount toCopyNRecords) {
    std::ifstream is(writeFromFilename, std::ios::binary);
    if (!is) { throw std::runtime_error("Error: Opening file " + writeFromFilename); }
    ByteCount bufSize = RoundUp(1024 * 1024, Config::RECORD_SIZE);
    char *buffer = new char[bufSize];
    ByteCount total = 0;
    while (is) {
        is.read(buffer, bufSize);
        ByteCount n = is.gcount();
        _os.write(buffer, n);
        total += n;
    }
    delete[] buffer;
    if (!_os) { throw std::runtime_error("Error: Writing to file"); }
    RowCount nRecords = total / Config::RECORD_SIZE;
    currSize += nRecords;
    printv("\t\t\t\tRunWriter copied %llu out of %llu records from %s to %s\n", nRecords,
           toCopyNRecords, writeFromFilename.c_str(), _filename.c_str());
    assert(toCopyNRecords == nRecords);
    return nRecords;
}

RowCount RunWriter::writeNextRun(Run &run) {
    char *data = run.getAllData();
    RowCount nRecords = run.getSize();
    _os.write(data, nRecords * Config::RECORD_SIZE);
    if (!_os) { throw std::runtime_error("Error: Writing to file"); }
    currSize += nRecords;
    delete[] data;
    return nRecords;
}
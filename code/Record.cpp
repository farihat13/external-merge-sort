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


// =========================================================
// ------------------------- RunReader ---------------------
// =========================================================


Record *RunReader::readNextRecords(RowCount *nRecords) {
    RowCount nRecordsToRead = *nRecords;
    RowCount nRecordsReadSoFar = 0;
    Record *head = new Record();
    Record *curr = head;

    // Read records page by page
    ByteCount nBytesToRead = this->PAGE_SIZE_IN_RECORDS * Config::RECORD_SIZE;
    char *recData = new char[nBytesToRead];
    while (nRecordsReadSoFar < nRecordsToRead) {

        if (_is.eof()) { break; }

        _is.read(recData, nBytesToRead);
        ByteCount nBytesRead = _is.gcount();

        if (nBytesRead == 0) { break; }

        if (nBytesRead % Config::RECORD_SIZE != 0) {
            delete[] recData;
            std::string msg = "Error: Read " + std::to_string(nBytesRead) +
                              " bytes, not aligned with record size";
            printv("%s\n", msg.c_str());
            throw std::runtime_error(msg);
        }

        // Create a linked list of records
        for (RowCount i = 0; i < nBytesRead / Config::RECORD_SIZE; i++) {
            Record *rec = new Record(recData + i * Config::RECORD_SIZE);
            curr->next = rec;
            curr = rec;
            nRecordsReadSoFar++;
        }
    }

    curr->next = nullptr;          // mark the end of the linked list
    *nRecords = nRecordsReadSoFar; // update the number of records read
    curr = head->next;             // skip the dummy head

    // Free memory
    delete head;      // delete the dummy head
    delete[] recData; // delete the buffer

    // Return the head of the linked list
    return curr;
}


// =========================================================
// ----------------------- RunWriter -----------------------
// =========================================================


RowCount RunWriter::writeFromFile(std::string writeFromFilename, RowCount toCopyNRecords) {

    // Open the given file
    std::ifstream is(writeFromFilename, std::ios::binary);
    if (!is) { throw std::runtime_error("Error: Opening file " + writeFromFilename); }

    // Copy the records from the given file to this writer's file
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

    // Update the writer's size
    RowCount nRecords = total / Config::RECORD_SIZE;
    currSize += nRecords;

    printv("\t\t\t\tRunWriter copied %llu out of %llu records from %s to %s\n", nRecords,
           toCopyNRecords, writeFromFilename.c_str(), _filename.c_str());

    // Check if the number of records copied is as expected
    assert(toCopyNRecords == nRecords);

    // Return the number of records copied
    return nRecords;

} // writeFromFile


RowCount RunWriter::writeNextRun(Run *run) {

    char *data = run->getAllData();
    RowCount nRecords = run->getSize();
    _os.write(data, nRecords * Config::RECORD_SIZE);
    if (!_os) { throw std::runtime_error("Error: Writing to file"); }
    currSize += nRecords;
    delete[] data;
    return nRecords;

} // writeNextRun
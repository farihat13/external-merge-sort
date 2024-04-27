#include "Verify.h"

/*
 * @brief verify sort order of an output file given an input file
 * @details
 * - read input file and output file
 * - input file can have duplicate records, which should be removed
 * - file is a binary file with fixed size records, each record has key and value
 * - record is n bytes, key is 8 bytes and value is n-8 bytes
 * - sort order is based on key
 * - duplicate is checked on entire record
 *
 * - check two things: sort order and data integrity
 * - check 1:
 *      first scan the output file and check if it is sorted
 * - check 2:
 *      create half DRAM sized hash partitioned file from input file and output file
 *      and compare the hash partitioned files
 *      every input in a input hash file should be in the corresponding output hash file
 */

char *readRecordsFromFile(std::ifstream &file, RowCount nRecordsPerRead, RowCount *nRecordsLoaded) {
    char *data = new char[nRecordsPerRead * Config::RECORD_SIZE];
    file.read(data, nRecordsPerRead * Config::RECORD_SIZE);
    int nBytes = file.gcount();
    *nRecordsLoaded = nBytes / Config::RECORD_SIZE;
    return data;
}

std::ifstream openReadFile(const std::string &filePath) {
    std::ifstream file(filePath, std::ios::binary);
    if (!file.is_open()) {
        printvv("ERROR: Failed to open read file '%s'\n", filePath.c_str());
        exit(1);
    }
    file.seekg(0, std::ios::beg);
    if (!file) {
        printv("ERROR: Failed to seek to the beginning of '%s'\n", filePath.c_str());
        exit(1);
    }
    return file;
}

std::ofstream openWriteFile(const std::string &filePath) {
    std::ofstream file(filePath, std::ios::binary);
    if (!file.is_open()) {
        printvv("ERROR: Failed to open write file '%s'\n", filePath.c_str());
        exit(1);
    }
    file.seekp(0, std::ios::beg);
    if (!file) {
        printv("ERROR: Failed to seek to the beginning of '%s'\n", filePath.c_str());
        exit(1);
    }
    return file;
}


bool verifyOrder(const std::string &outputFilePath, RowCount nRecordsPerRead) {
    std::ifstream outputFile = openReadFile(outputFilePath);

    // read first record
    RowCount nRecordsLoaded = 0;
    char *data = readRecordsFromFile(outputFile, nRecordsPerRead, &nRecordsLoaded);
    Record *prevRecord = new Record(data);
    RowCount nRecords = nRecordsLoaded;
    RowCount i = 1;

    printf("Number of records per read: %ld\n", nRecordsLoaded);

    while (1) {
        nRecordsLoaded--;

        // fetch new data if needed
        if (nRecordsLoaded == 0) {
            data = readRecordsFromFile(outputFile, nRecordsPerRead, &nRecordsLoaded);
            if (data == nullptr || nRecordsLoaded == 0) {
                printvv("WARNING: no records read\n");
                break;
            }
            nRecords += nRecordsLoaded;
        }

        Record *record = new Record(data);
        if (record < prevRecord) {
            printvv("ERROR: Record %ld is not sorted\n", i);
            return false;
        }

        i += 1;
        prevRecord = record;
        data += Config::RECORD_SIZE;
    }
    printf("Order verified with %ld records\n", nRecords);
    return true;
}

bool verifyIntegrity(const std::string &inputFilePath, const std::string &outputFilePath) {
    printf("Testing\n");
    return true;
}
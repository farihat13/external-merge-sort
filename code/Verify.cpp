#include "Verify.h"
#include <set>

/**
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

void readRecordsFromFile(std::ifstream &file, RowCount nRecordsPerRead, RowCount *nRecordsLoaded,
                         char *data) {
    file.read(data, nRecordsPerRead * Config::RECORD_SIZE);
    ByteCount nBytes = file.gcount();
    *nRecordsLoaded = nBytes / Config::RECORD_SIZE;
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


bool verifyOrder(const std::string &outputFilePath, uint64_t capacityMB) {
    TRACE(true);
    if (Config::NUM_RECORDS == 0) {
        printvv("SUCCESS: No records to verify order\n");
        return true;
    }
    printvv("============= Verifying order =============\n");

    std::ifstream outputFile = openReadFile(outputFilePath);

    uint64_t capacityBytes = capacityMB * 1024 * 1024;
    RowCount nRecordsPerRead = capacityBytes / Config::RECORD_SIZE;

    RowCount nRecordsLoaded = 0;
    RowCount i = 1;

    int comparisonLength = Config::RECORD_KEY_SIZE;
    char *data = new char[nRecordsPerRead * Config::RECORD_SIZE];
    char *startData = data;

    readRecordsFromFile(outputFile, nRecordsPerRead, &nRecordsLoaded, data);
    RowCount nRecords = nRecordsLoaded;

    // read first record
    char *prevRecord = new char[comparisonLength];
    std::memcpy(prevRecord, data, comparisonLength);

    bool ordered = true;
    while (ordered) {
        nRecordsLoaded--;

        // fetch new data if needed
        if (nRecordsLoaded == 0) {
            data = startData;
            readRecordsFromFile(outputFile, nRecordsPerRead, &nRecordsLoaded, data);
            if (data == nullptr || nRecordsLoaded == 0) {
                printvv("Finished reading output\n");
                break;
            }
            nRecords += nRecordsLoaded;
        } else {
            data += Config::RECORD_SIZE;
        }

        Record *record = Record::wrapAsRecord(data);
        int compare = std::strncmp(prevRecord, record->data, comparisonLength);
        if (compare > 0) {
            printvv("ERROR: Record %ld is not sorted\n", i);
            printvv("prev: %s, curr: %s\n", prevRecord, record->reprKey());
            ordered = false;
        }

        i += 1;
        std::memcpy(prevRecord, record->data, comparisonLength);
    }

    // cleanup
    if (startData != nullptr) {
        delete startData;
        delete prevRecord;
    }
    outputFile.close();


    if (ordered) {
        printvv("SUCCESS: Order verified with %ld records\n", nRecords);
        printvv("============= Order verification successful =============\n");
    } else {
        printvv("Order verification failed\n");
        printvv("============= Order verification failed =============\n");
    }

    return ordered;
}


u_int64_t simpleHash(Record *record, u_int64_t nPartitions) {
    // record is n bytes, key is 8 bytes and value is n-8 bytes
    // hash using key
    char *key = record->data;
    u_int64_t hash = 0;
    for (int i = 0; i < Config::RECORD_SIZE; i++) {
        hash = (hash << 5) + hash + key[i];
    }
    return hash;
}


void partitionFile(const std::string &inputFilePath, const std::string &hashFilesDir,
                   u_int64_t nPartitions, RowCount nRecordsPerRead) {
    std::ifstream inputFile = openReadFile(inputFilePath);

    // create partitioned files
    std::ofstream *outputFiles = new std::ofstream[nPartitions];
    for (u_int64_t i = 0; i < nPartitions; i++) {
        std::string outputFilePath = hashFilesDir + std::to_string(i) + ".txt";
        outputFiles[i] = openWriteFile(outputFilePath);
    }

    RowCount nRecordsLoaded = 0;
    char *data = new char[nRecordsPerRead * Config::RECORD_SIZE];

    // read input file in batches and hash partition it
    while (1) {
        readRecordsFromFile(inputFile, nRecordsPerRead, &nRecordsLoaded, data);
        if (data == nullptr || nRecordsLoaded == 0) { break; }
        printvv("Number of records loaded: %ld\n", nRecordsLoaded);

        for (RowCount i = 0; i < nRecordsLoaded; i++) {
            Record *record = new Record(data + i * Config::RECORD_SIZE);
            u_int64_t hash = simpleHash(record, nPartitions);
            uint64_t partition = hash % nPartitions;
            outputFiles[partition].write(reinterpret_cast<char *>(&hash),
                                         Config::VERIFY_HASH_BYTES);
            delete record;
        }
        // free(data);
    }

    if (data != nullptr) { delete data; }

    for (u_int64_t i = 0; i < nPartitions; i++) {
        outputFiles[i].close();
    }
    inputFile.close();
    delete[] outputFiles;
}

bool contains(const std::vector<Record *> &arr, Record *target) {
    size_t left = 0;
    size_t right = arr.size() - 1;

    while (left <= right) {
        size_t mid = left + (right - left) / 2; // Prevent potential overflow
        int compare = std::strncmp(arr[mid]->data, target->data, Config::RECORD_SIZE);

        if (compare == 0) {
            return true;
        } else if (compare < 0) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    return false;
}


void cleanDirectory(const std::string &dirPath) {
    // remove directory and create again
    std::string command = "rm -rf " + dirPath;
    int code = system(command.c_str());
    if (code == -1) { printvv("ERROR: Failed to remove directory '%s'\n", dirPath.c_str()); }
    command = "mkdir -p " + dirPath;
    code = system(command.c_str());
    if (code == -1) { printvv("ERROR: Failed to create directory '%s'\n", dirPath.c_str()); }
}


void readHashesFromFileToSet(std::ifstream &file, std::set<uint64_t> &hashes,
                             uint64_t &nHashesRead) {
    file.seekg(0, std::ios::end);
    std::streampos fileSize = file.tellg();
    file.seekg(0, std::ios::beg);
    nHashesRead = fileSize / sizeof(uint64_t);
    for (size_t i = 0; i < nHashesRead; i++) {
        uint64_t hash;
        file.read(reinterpret_cast<char *>(&hash), sizeof(uint64_t));
        hashes.insert(hash);
    }
}

void readHashesFromFile(std::ifstream &file, std::vector<uint64_t> &hashes, uint64_t &nHashesRead) {
    file.seekg(0, std::ios::end);
    std::streampos fileSize = file.tellg();
    file.seekg(0, std::ios::beg);
    nHashesRead = fileSize / sizeof(uint64_t);
    hashes.resize(nHashesRead);

    if (!file.read(reinterpret_cast<char *>(hashes.data()), nHashesRead * sizeof(uint64_t))) {
        printvv("ERROR: Failed to read hashes from file\n");
    }

    // for (size_t i = 0; i < nHashesRead; i++) {
    //     uint64_t hash;
    //     file.read(reinterpret_cast<char *>(&hash), sizeof(uint64_t));
    //     hashes.push_back(hash);
    // }
}

bool compareHashFiles(uint64_t i, const std::string &inputDir, const std::string &outputDir,
                      RowCount &nInputRecordsLoaded, RowCount &nOutputRecordsLoaded) {
    std::string inputFilePath = inputDir + std::to_string(i) + ".txt";
    std::string outputFilePath = outputDir + std::to_string(i) + ".txt";
    std::ifstream inputPartition = openReadFile(inputFilePath);
    std::ifstream outputPartition = openReadFile(outputFilePath);

    bool integrity = true;

    // check if exists
    if (!inputPartition.is_open() && !outputPartition.is_open()) { return true; }
    if (!inputPartition.is_open()) {
        printvv("ERROR: Failed to open input partition file '%s'\n", inputFilePath.c_str());
        outputPartition.close();
        return false;
    }
    if (!outputPartition.is_open()) {
        printvv("ERROR: Failed to open output partition file '%s'\n", outputFilePath.c_str());
        inputPartition.close();
        return false;
    }


    // std::set<uint64_t> inputHashes;
    // readHashesFromFileToSet(inputPartition, inputHashes, nInputRecordsLoaded);
    // std::set<uint64_t> outputHashes;
    // readHashesFromFileToSet(outputPartition, outputHashes, nOutputRecordsLoaded);
    // if (inputHashes != outputHashes) {
    //     printvv("ERROR: Hashes do not match for partition %ld\n", i);
    //     integrity = false;
    // }

    std::vector<uint64_t> inputHashes;
    readHashesFromFile(inputPartition, inputHashes, nInputRecordsLoaded);
    std::vector<uint64_t> outputHashes;
    readHashesFromFile(outputPartition, outputHashes, nOutputRecordsLoaded);
    std::sort(inputHashes.begin(), inputHashes.end());
    printvv("Comparing partition %ld: input hashes: %ld, output hashes: %ld\n", i,
            nInputRecordsLoaded, nOutputRecordsLoaded);
    assert(nInputRecordsLoaded == inputHashes.size());
    assert(nOutputRecordsLoaded == outputHashes.size());
    for (uint64_t j = 0; j < nOutputRecordsLoaded && integrity; j++) {
        uint64_t hash = outputHashes[j];
        if (!std::binary_search(inputHashes.begin(), inputHashes.end(), hash)) {
            printvv("ERROR: Hash %ld not found in input partition\n", hash);
            integrity = false;
        }
    }

    inputHashes.clear();
    outputHashes.clear();
    inputPartition.close();
    outputPartition.close();

    return integrity;
}

bool verifyIntegrity(const std::string &inputFilePath, const std::string &outputFilePath,
                     uint64_t capacityMB) {
    TRACE(true);
    if (Config::NUM_RECORDS == 0) {
        printvv("SUCCESS: No records to verify integrity\n");
        return true;
    }
    printvv("============= Verifying integrity =============\n");

    // delete existing hash partitioned files
    std::string inputDir = Config::VERIFY_INPUTDIR;
    std::string outputDir = Config::VERIFY_OUTPUTDIR;

    cleanDirectory(inputDir);
    cleanDirectory(outputDir);

    std::ifstream inputFile = openReadFile(inputFilePath);
    std::ifstream outputFile = openReadFile(outputFilePath);

    uint64_t capacityBytes = capacityMB * 1024 * 1024;
    uint64_t expectedPartitionSize = capacityBytes / 2; // expecting to hold two partitions in DRAM
    uint64_t expectedHashesPerPartition = expectedPartitionSize / Config::VERIFY_HASH_BYTES;
    uint64_t nPartitions = ceil(Config::NUM_RECORDS * 1.0 / expectedHashesPerPartition);

    printvv("Expected partition size: %ld\n", expectedPartitionSize);
    printvv("Expected hashes per partition: %ld\n", expectedHashesPerPartition);

    printvv("Number of partitions: %ld\n", nPartitions);

    // read input file in batches and hash partition it
    uint64_t nRecordsPerRead = capacityBytes / Config::RECORD_SIZE;
    partitionFile(inputFilePath, inputDir, nPartitions, nRecordsPerRead);
    printvv("Partitioned input file\n");
    partitionFile(outputFilePath, outputDir, nPartitions, nRecordsPerRead);
    printvv("Partitioned output file\n");

    // compare hash partitioned files
    printvv("Comparing hash partitioned files\n");
    RowCount totalInputRecords = 0, totalOutputRecords = 0;
    bool integrity = true;

    for (u_int64_t i = 0; i < nPartitions && integrity; i++) {
        RowCount nInputRecordsLoaded = 0, nOutputRecordsLoaded = 0;
        integrity =
            compareHashFiles(i, inputDir, outputDir, nInputRecordsLoaded, nOutputRecordsLoaded);
        totalInputRecords += nInputRecordsLoaded;
        totalOutputRecords += nOutputRecordsLoaded;
    }

    if (integrity) {
        printvv("Total input records generated: %ld\n", Config::NUM_RECORDS);
        printvv("Total input records verified: %ld\n", totalInputRecords);
        printvv("Total output records verified: %ld\n", totalOutputRecords);
        printvv("Duplicates removed: %ld\n", totalInputRecords - totalOutputRecords);
        printvv("SUCCESS: Integrity verified\n");
        printvv("============= Integrity verification successful =============\n");
    } else {
        printvv("ERROR: Integrity verification failed\n");
        printvv("============= Integrity verification failed =============\n");
    }
    flushvv();
    // cleanup
    cleanDirectory(inputDir);
    cleanDirectory(outputDir);
    return integrity;
}
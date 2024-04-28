#include "Verify.h"

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

char *readRecordsFromFile(std::ifstream &file, RowCount nRecordsPerRead, RowCount *nRecordsLoaded) {
    char *data = new char[nRecordsPerRead * Config::RECORD_SIZE];
    file.read(data, nRecordsPerRead * Config::RECORD_SIZE);
    ByteCount nBytes = file.gcount();
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


bool verifyOrder(const std::string &outputFilePath, uint64_t capacityMB) {
    printf("============= Verifying order =============\n");

    std::ifstream outputFile = openReadFile(outputFilePath);

    uint64_t capacityBytes = capacityMB * 1024 * 1024;
    RowCount nRecordsPerRead = capacityBytes / Config::RECORD_SIZE;

    // read first record
    RowCount nRecordsLoaded = 0;
    char *data = readRecordsFromFile(outputFile, nRecordsPerRead, &nRecordsLoaded);
    Record *prevRecord = new Record(data);
    RowCount nRecords = nRecordsLoaded;
    RowCount i = 1;

    printf("Number of records per read: %ld\n", nRecordsLoaded);
    bool ordered = true;

    while (ordered) {
        nRecordsLoaded--;

        // fetch new data if needed
        if (nRecordsLoaded == 0) {
            data = readRecordsFromFile(outputFile, nRecordsPerRead, &nRecordsLoaded);
            if (data == nullptr || nRecordsLoaded == 0) {
                printf("Finished reading output\n");
                break;
            }
            nRecords += nRecordsLoaded;
        }

        Record *record = new Record(data);
        int compare = std::strncmp(prevRecord->data, record->data, Config::RECORD_KEY_SIZE);
        if (compare > 0) {
            printvv("ERROR: Record %ld is not sorted\n", i);
            printvv("prev: %s, curr: %s\n", prevRecord->reprKey(), record->reprKey());
            ordered = false;
        }

        i += 1;
        free(prevRecord);
        prevRecord = record;
        data += Config::RECORD_SIZE;
    }

    // cleanup
    free(data);
    outputFile.close();


    if (ordered) {
        printf("Order verified with %ld records\n", nRecords);
        printf("============= Order verification successful =============\n");
    } else {
        printf("Order verification failed\n");
        printf("============= Order verification failed =============\n");
    }

    return ordered;
}


u_int64_t hash(Record *record, u_int64_t nPartitions) {
    // record is n bytes, key is 8 bytes and value is n-8 bytes
    // hash using key
    char *key = record->reprKey();
    u_int64_t hash = 0;
    for (int i = 0; i < 8; i++) {
        hash = (hash << 5) + hash + key[i];
    }
    delete[] key;
    return hash % nPartitions;
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
    char *data;

    // read input file in batches and hash partition it
    while (1) {
        data = readRecordsFromFile(inputFile, nRecordsPerRead, &nRecordsLoaded);
        if (data == nullptr || nRecordsLoaded == 0) { break; }
        printf("Number of records loaded: %ld\n", nRecordsLoaded);

        for (RowCount i = 0; i < nRecordsLoaded; i++) {
            Record *record = new Record(data + i * Config::RECORD_SIZE);
            u_int64_t partition = hash(record, nPartitions);
            outputFiles[partition].write(record->data, Config::RECORD_SIZE);
            // free(record);
            record->data = nullptr;
        }
        if (data != nullptr) { free(data); }
        // free(data);
    }

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

bool compareHashFiles(uint64_t i, const std::string &inputDir, const std::string &outputDir,
                      uint64_t nRecordsPerRead, RowCount &nInputRecordsLoaded,
                      RowCount &nOutputRecordsLoaded) {
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

    char *inputData = readRecordsFromFile(inputPartition, nRecordsPerRead, &nInputRecordsLoaded);
    char *outputData = readRecordsFromFile(outputPartition, nRecordsPerRead, &nOutputRecordsLoaded);

    printf("Number of records loaded for partition %ld: %ld %ld\n", i, nInputRecordsLoaded,
           nOutputRecordsLoaded);

    std::vector<Record *> outputRecordsVec;
    for (uint64_t j = 0; j < nOutputRecordsLoaded; j++) {
        Record *record = new Record(outputData + j * Config::RECORD_SIZE);
        outputRecordsVec.push_back(record);
    }

    for (uint64_t j = 0; j < nInputRecordsLoaded && integrity; j++) {
        Record *record = new Record(inputData + j * Config::RECORD_SIZE);
        bool found = contains(outputRecordsVec, record);

        if (!found) {
            printvv("ERROR: Record %ld not found in output partition\n", j);
            printf("Record: %s\n", record->repr());
            integrity = false;
        }
        record->data = nullptr;
    }

    for (uint64_t j = 0; j < nOutputRecordsLoaded; j++) {
        free(outputRecordsVec[j]);
    }

    free(outputData);
    free(inputData);
    inputPartition.close();
    outputPartition.close();

    return integrity;
}

bool verifyIntegrity(const std::string &inputFilePath, const std::string &outputFilePath,
                     uint64_t capacityMB) {

    printf("============= Verifying integrity =============\n");

    // delete existing hash partitioned files
    std::string inputDir = Config::VERIFY_INPUTDIR;
    std::string outputDir = Config::VERIFY_OUTPUTDIR;
    cleanDirectory(inputDir);
    cleanDirectory(outputDir);


    std::ifstream inputFile = openReadFile(inputFilePath);
    std::ifstream outputFile = openReadFile(outputFilePath);

    uint64_t capacityBytes = capacityMB * 1024 * 1024;
    uint64_t matchCapacityBytes = capacityBytes / 2;

    // expecting partition of this size matchCapacityBytes
    // handle skew, reducing partition size to half
    uint64_t expectedPartitionBytes = matchCapacityBytes / 2;
    uint64_t expectedRecordsPerPartition = expectedPartitionBytes / Config::RECORD_SIZE;
    uint64_t nPartitions = ceil(Config::NUM_RECORDS * 1.0 / expectedRecordsPerPartition);

    printf("Number of partitions: %ld\n", nPartitions);

    // read input file in batches and hash partition it
    uint64_t nRecordsPerRead = matchCapacityBytes / Config::RECORD_SIZE;
    partitionFile(inputFilePath, inputDir, nPartitions, nRecordsPerRead);
    printf("Partitioned input file\n");
    partitionFile(outputFilePath, outputDir, nPartitions, nRecordsPerRead);
    printf("Partitioned output file\n");

    // compare hash partitioned files
    printf("Comparing hash partitioned files\n");
    RowCount totalInputRecords = 0, totalOutputRecords = 0;
    bool integrity = true;
    for (u_int64_t i = 0; i < nPartitions && integrity; i++) {
        RowCount nInputRecordsLoaded = 0, nOutputRecordsLoaded = 0;
        integrity = compareHashFiles(i, inputDir, outputDir, nRecordsPerRead, nInputRecordsLoaded,
                                     nOutputRecordsLoaded);
        totalInputRecords += nInputRecordsLoaded;
        totalOutputRecords += nOutputRecordsLoaded;
    }

    if (integrity) {
        printf("Total input records generated: %ld\n", Config::NUM_RECORDS);
        printf("Total input records verified: %ld\n", totalInputRecords);
        printf("Total output records verified: %ld\n", totalOutputRecords);
        printf("Duplicates removed: %ld\n", totalInputRecords - totalOutputRecords);
        printf("SUCCESS: Integrity verified\n");
        printf("============= Integrity verification successful =============\n");
    } else {
        printf("ERROR: Integrity verification failed\n");
        printf("============= Integrity verification failed =============\n");
    }
    // cleanup
    cleanDirectory(inputDir);
    cleanDirectory(outputDir);
    return integrity;
}
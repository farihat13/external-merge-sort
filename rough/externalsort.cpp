#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <ctime>

struct Record {
    int key;
    std::vector<char> data;
};

bool compareRecords(const Record &a, const Record &b) { return a.key < b.key; }

void generateInputFile(const std::string &filename, int recordSize,
                       int numRecords) {
    std::ofstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Error opening file for writing." << std::endl;
        exit(1);
    }

    srand(time(nullptr)); // Seed random number generator

    for (int i = 0; i < numRecords; ++i) {
        int key = rand();
        file.write(reinterpret_cast<char *>(&key), sizeof(key));

        for (int j = sizeof(key); j < recordSize; ++j) {
            char randomByte = rand() % 256;
            file.write(&randomByte, sizeof(randomByte));
        }
    }

    file.close();
}

void externalSort(const std::string &inputFile, const std::string &outputFile,
                  int recordSize) {
    std::ifstream file(inputFile, std::ios::binary);
    if (!file) {
        std::cerr << "Error opening input file." << std::endl;
        exit(1);
    }

    // Read records into memory
    std::vector<Record> records;
    while (file) {
        Record record;
        file.read(reinterpret_cast<char *>(&record.key), sizeof(record.key));

        if (!file)
            break; // End of file reached

        record.data.resize(recordSize - sizeof(record.key));
        file.read(record.data.data(), record.data.size());

        records.push_back(record);
    }

    file.close();

    // Sort records based on key
    std::sort(records.begin(), records.end(), compareRecords);

    // Write sorted records to output file
    std::ofstream outputFileStream(outputFile, std::ios::binary);
    if (!outputFileStream) {
        std::cerr << "Error opening output file." << std::endl;
        exit(1);
    }

    for (const auto &record : records) {
        outputFileStream.write(reinterpret_cast<const char *>(&record.key),
                               sizeof(record.key));
        outputFileStream.write(record.data.data(), record.data.size());
    }

    outputFileStream.close();
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0]
                  << " <record size> <number of records> <output file>"
                  << std::endl;
        return 1;
    }

    int recordSize = std::atoi(argv[1]);
    int numRecords = std::atoi(argv[2]);
    std::string outputFile = argv[3];

    std::string inputFile = "input.bin";

    generateInputFile(inputFile, recordSize, numRecords);
    externalSort(inputFile, outputFile, recordSize);

    return 0;
}

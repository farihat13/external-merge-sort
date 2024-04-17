#include "common.h"
#include "storage.h"
#include <algorithm>
#include <cassert>
#include <climits>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>


/**
 * @brief read command line arguments and update Config class
 *
 * @param argc
 * @param argv
 */
void readCmdlineArgs(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr,
                "Usage: %s -c <num_records> -s <record_size> -o <trace_file>\n",
                argv[0]);
        exit(1);
    }

    int num_records = 0;
    int record_size = 0;
    std::string trace_file = "";
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-c") == 0) {
            if (i + 1 < argc) {
                num_records = std::atoi(argv[++i]);
            } else {
                fprintf(stderr, "Option -c requires an argument.\n");
                exit(1);
            }
        } else if (strcmp(argv[i], "-s") == 0) {
            if (i + 1 < argc) {
                record_size = std::atoi(argv[++i]);
            } else {
                fprintf(stderr, "Option -s requires an argument.\n");
                exit(1);
            }
        } else if (strcmp(argv[i], "-o") == 0) {
            if (i + 1 < argc) {
                trace_file = argv[++i];
            } else {
                fprintf(stderr, "Option -o requires an argument.\n");
                exit(1);
            }
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            exit(1);
        }
    }
    Config::RECORD_SIZE = record_size;
    Config::NUM_RECORDS = num_records;
    Config::TRACE_FILE = trace_file;
} // readCmdlineArgs


void gen_a_record(char *s, const int len) {
    static const char alphanum[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
} // gen_a_record

void generateInputFile(const std::string &filename) {
    std::ofstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Error opening file for writing." << std::endl;
        exit(1);
    }
    srand(time(nullptr)); // Seed random number generator
#if defined(_SEED)
    srand(0);
#endif
    char *record = new char[Config::RECORD_SIZE];
    for (long long i = 0; i < Config::NUM_RECORDS; ++i) {
        gen_a_record(record, Config::RECORD_SIZE);
        record[Config::RECORD_SIZE - 1] = '\n'; // TODO: remove later
        file.write(record, Config::RECORD_SIZE);
    }
    // clean up
    file.close();
    delete[] record;
    printvv("Generated %lld records in file %s\n", Config::NUM_RECORDS,
            filename.c_str());

#if defined(_DEBUG)
    long long expected = Config::NUM_RECORDS * Config::RECORD_SIZE;
    std::ifstream inputfile(filename, std::ios::binary);
    inputfile.seekg(0, std::ios::end);
    long long fileSize = inputfile.tellg();
    printv("File size: %lld, Expected %lld\n", fileSize, expected);
    DebugAssert(expected == fileSize);
    inputfile.close();
#endif
}


/**
 * @brief Main function
 * ./externalsort -c 20 -s 1024 -o trace
 */
int main(int argc, char *argv[]) {
    // read command line arguments
    readCmdlineArgs(argc, argv);

    // read config file
#if defined(_SMALLCONFIG) && defined(_DEBUG)
    readConfig("config_small.txt");
#endif

    // calculate derived config values and print config
    printConfig();
    // generate input file
    generateInputFile(Config::INPUT_FILE);
    flushv();

    externalSort();
    flushv();

    return 0;
}

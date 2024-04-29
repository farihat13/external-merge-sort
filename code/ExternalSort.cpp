#include "Filter.h"
#include "Iterator.h"
#include "Scan.h"
#include "Sort.h"
#include "Verify.h"
#include "config.h"
#include "defs.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>


// ============================================================================
// ----------------------------- Command Line Arguments -----------------------
// ============================================================================

/**
 * @brief read command line arguments and update Config class
 *  `-c` number of records
 *  `-s` size of each record
 *  `-o` output file
 *  `-v` verify the output file
 *  `-vo` verify the output file only`
 *
 * ./ExternalSort.exe -c 20 -s 1024 -o trace0.txt
 * @param argc
 * @param argv
 */
void readCmdlineArgs(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s -c <num_records> -s <record_size> -o <trace_file>\n", argv[0]);
        exit(1);
    }

    int num_records = 0;
    int record_size = 0;
    std::string trace_file = "";
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-c") == 0) {
            if (i + 1 < argc) {
                num_records = std::atoi(argv[++i]);
                Config::NUM_RECORDS = num_records;
            } else {
                fprintf(stderr, "Option -c requires an argument.\n");
                exit(1);
            }
        } else if (strcmp(argv[i], "-s") == 0) {
            if (i + 1 < argc) {
                record_size = std::atoi(argv[++i]);
                Config::RECORD_SIZE = record_size;
            } else {
                fprintf(stderr, "Option -s requires an argument.\n");
                exit(1);
            }
        } else if (strcmp(argv[i], "-o") == 0) {
            if (i + 1 < argc) {
                trace_file = argv[++i];
                Config::TRACE_FILE = trace_file;
            } else {
                fprintf(stderr, "Option -o requires an argument.\n");
                exit(1);
            }
        } else if (strcmp(argv[i], "-vo") == 0) {
            Config::VERIFY_ONLY = true;
        } else if (strcmp(argv[i], "-v") == 0) {
            Config::VERIFY = true;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            exit(1);
        }
    }
    Config::INPUT_FILE = "input-c" + std::to_string(Config::NUM_RECORDS) + "-s" +
                         std::to_string(Config::RECORD_SIZE) + ".txt";
} // readCmdlineArgs


/**
 * initialize at the very beginning, just after processing command line arguments
 */
void init() {
#if defined(_SMALL)
    Config::DRAM_CAPACITY = 1LL * 5 * 1024 * 1024; // 10 MB
    Config::SSD_CAPACITY = 1LL * 10 * 1024 * 1024; // 25 MB
    Config::RECORD_SIZE = 1024;                    // 1024 bytes
    Config::NUM_RECORDS = 10000;                   // 10000 records
    Config::INPUT_FILE = "input-c" + std::to_string(Config::NUM_RECORDS) + "-s" +
                         std::to_string(Config::RECORD_SIZE) + ".txt";
    Config::VERIFY = true;
    printvv("WARNING: Running in SMALL mode\n");
    flushvv();
#elif defined(_BIG)
    Config::SSD_CAPACITY = 20LL * 1024 * 1024 * 1024; // 25 MB
    Config::RECORD_SIZE = 1024;                       // 1024 bytes
    Config::NUM_RECORDS = 11000000;                   // 10000 records
    Config::INPUT_FILE = "input-c" + std::to_string(Config::NUM_RECORDS) + "-s" +
                         std::to_string(Config::RECORD_SIZE) + ".txt";
    Config::VERIFY = false;
    printvv("WARNING: Running in BIG mode\n");
    flushvv();
#endif
    printConfig();
    HDD::getInstance();
    SSD::getInstance();
    DRAM::getInstance();
    printf("init done\n");
}


/**
 * @brief cleanup at the very end, what was initialized in init()
 */
void cleanup() {
    delete getMaxRecord();
    DRAM::deleteInstance();
    printv("deleted DRAM\n");
    flushv();
    SSD::deleteInstance();
    printf("deleted SSD\n");
    flushv();
    HDD::deleteInstance();
    printf("deleted HDD\n");
    flushv();
    printf("cleanup done\n");
}


int main(int argc, char *argv[]) {

    // read command line arguments
    readCmdlineArgs(argc, argv);

    if (!Config::VERIFY_ONLY) {
        // initialize anything needed
        init();

        Plan *scanPlan = new ScanPlan(Config::NUM_RECORDS, Config::INPUT_FILE);
        Plan *const plan = new SortPlan(scanPlan);

        Iterator *const it = plan->init();
        it->run();

        delete it;
        delete plan;

        // cleanup what was initialized in init()
        cleanup();
    }

    if (Config::VERIFY_ONLY || Config::VERIFY) {
        uint64_t capacityMB = 1024; // 1 GB or 1024 memory used for verification
        verifyOrder(Config::OUTPUT_FILE, capacityMB);
        verifyIntegrity(Config::INPUT_FILE, Config::OUTPUT_FILE, capacityMB);
    }
    return 0;


    return 0;
} // main
#include "Filter.h"
#include "Iterator.h"
#include "Scan.h"
#include "Sort.h"
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
    Config::INPUT_FILE = "input-c" + std::to_string(Config::NUM_RECORDS) + "-s" +
                         std::to_string(Config::RECORD_SIZE) + ".txt";
} // readCmdlineArgs


/**
 * initialize at the very beginning, just after processing command line arguments
 */
void init() {
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
    // delete HDD::getInstance();
    // printv("deleted HDD\n");
    // delete SSD::getInstance();
    // printv("deleted SSD\n");
    // delete DRAM::getInstance();
    // printv("deleted DRAM\n");
    delete getMaxRecord();
    printf("cleanup done\n");
}


int main(int argc, char *argv[]) {

    // read command line arguments
    readCmdlineArgs(argc, argv);

    // initialize anything needed
    init();

    // TRACE(true);

    // Plan *const plan = new SortPlan(new ScanPlan(Config::NUM_RECORDS));
    // new ScanPlan(Config::NUM_RECORDS);
    // new SortPlan ( new FilterPlan ( new ScanPlan (7) ) );
    Plan *const plan = new SortPlan(new ScanPlan(Config::NUM_RECORDS, Config::INPUT_FILE));

    Iterator *const it = plan->init();
    it->run();

    delete it;
    delete plan;

    // cleanup what was initialized in init()
    cleanup();

    return 0;
} // main
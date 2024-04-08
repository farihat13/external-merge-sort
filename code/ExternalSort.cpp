#include "Iterator.h"
#include "Scan.h"
#include "Filter.h"
#include "Sort.h"
#include "defs.h"
#include "config.h"
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <string>


/**
 *
 * @ brief Read command line arguments
 * `-c` number of records
 * `-s` size of each record
 * `-o` output file
 * ./ExternalSort.exe -c 20 -s 1024 -o trace0.txt
 * @ return Input
 */
void read_cmdline_arguments(int argc, char *argv[]) {
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

} // read_cmdline_arguments


int main(int argc, char *argv[]) {

    read_cmdline_arguments(argc, argv);
    Config::print_config();
    Logger::init(Config::TRACE_FILE);


    TRACE(true);

    Plan *const plan = new SortPlan(new ScanPlan(Config::NUM_RECORDS));
    // new ScanPlan(Config::NUM_RECORDS);
    // new SortPlan ( new FilterPlan ( new ScanPlan (7) ) );

    Iterator *const it = plan->init();
    it->run();

    delete it;
    delete plan;
    Logger::close();

    return 0;
} // main

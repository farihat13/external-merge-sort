#include "Iterator.h"
#include "Scan.h"
#include "Filter.h"
#include "Sort.h"
#include "params.h"

#include <cstring>


/**
 *
 * @ brief Read command line arguments
 * `-c` number of records
 * `-s` size of each record
 * `-o` output file
 * ./ExternalSort.exe -c 20 -s 1024 -o trace0.txt
 * @ return Input
 */
Input *read_cmdline_arguments(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(
            stderr,
            "Usage: %s -c <num_records> -s <record_size> -o <output_file>\n",
            argv[0]);
        exit(1);
    }

    int num_records = 0;
    int record_size = 0;
    std::string output_file = "";
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
                output_file = argv[++i];
            } else {
                fprintf(stderr, "Option -o requires an argument.\n");
                exit(1);
            }
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            exit(1);
        }
    }

    Input *input = new Input(num_records, record_size, output_file);
    return input;
} // read_cmdline_arguments


int main(int argc, char *argv[]) {

    Input *input = read_cmdline_arguments(argc, argv);
    // input->print();
    // StorageConfig::configToString();


    TRACE(true);

    Plan *const plan = new ScanPlan(7);
    // new SortPlan ( new FilterPlan ( new ScanPlan (7) ) );

    Iterator *const it = plan->init();
    it->run();
    delete it;

    delete plan;

    return 0;
} // main

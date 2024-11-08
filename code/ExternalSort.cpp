#include "Filter.h"
#include "Iterator.h"
#include "Scan.h"
#include "Sort.h"
#include "Verify.h"
#include "config.h"
#include "defs.h"
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
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
    std::string usage = "Usage: " + std::string(argv[0]) +
                        " -c <num_records> -s <record_size> -o <trace_file> -v <verify_output> -vo "
                        "<verify_only> \n";
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
            fprintf(stderr, "Usage: %s -c <num_records> -s <record_size> -o <trace_file>\n",
                    argv[0]);
            exit(1);
        }
    }
    Config::INPUT_FILE = "input-c" + std::to_string(Config::NUM_RECORDS) + "-s" +
                         std::to_string(Config::RECORD_SIZE) + ".txt";
    Config::OUTPUT_FILE = "output-c" + std::to_string(Config::NUM_RECORDS) + "-s" +
                          std::to_string(Config::RECORD_SIZE) + ".txt";
} // readCmdlineArgs


/**
 * initialize at the very beginning, just after processing command line arguments
 */
void init() {
#if defined(_SMALL)
    Config::DRAM_CAPACITY = 1LL * 3 * 1024 * 1024;   // 5 MB
    Config::SSD_CAPACITY = 1LL * 1024 * 1024 * 1024; // 1 GB
    Config::RECORD_SIZE = 1024;                      // 1024 bytes
    Config::NUM_RECORDS = 800 * 1024;                // 10000 records
    Config::INPUT_FILE = "input-c" + std::to_string(Config::NUM_RECORDS) + "-s" +
                         std::to_string(Config::RECORD_SIZE) + ".txt";
    // Config::VERIFY = true;
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


    if (Config::RECORD_SIZE < Config::RECORD_KEY_SIZE) {
        std::string msg = "Error: Record size is less than key size";
        throw std::runtime_error(msg);
    }

    // Check if input and output file exists
    if (Config::VERIFY_ONLY) {
        if (!std::ifstream(Config::INPUT_FILE.c_str())) {
            std::string msg =
                "Error: Verify-only selected. Input file " + Config::INPUT_FILE + " does not exist";
            throw std::runtime_error(msg);
        }
        if (!std::ifstream(Config::OUTPUT_FILE.c_str())) {
            std::string msg = "Error: Verify-only selected. Output file " + Config::OUTPUT_FILE +
                              " does not exist";
            throw std::runtime_error(msg);
        }
        printvv("INFO: Verify-only selected\n");
        flushvv();
        return;
    }

    HDD *_hdd = HDD::getInstance();
    HDD *_ssd = SSD::getInstance();
    DRAM *_dram = DRAM::getInstance();
    RowCount _ssdPageSize = _ssd->getPageSizeInRecords();
    RowCount _dramCapacity = _dram->getCapacityInRecords();
    RowCount _hddPageSize = _hdd->getPageSizeInRecords();
    RowCount _ssdCapacity = _ssd->getCapacityInRecords();

    /*
    Assuming, minMergeFanIn = 4, minMergeFanOut = 2;
    DRAM capacity should be able to hold at least 4 ssd pages;
    SSD capacity should be able to hold at least 4 hdd pages
    */
    int minMergeFanIn = 2;
    int minMergeFanOut = 2;
    if ((minMergeFanIn + minMergeFanOut) * _ssdPageSize > _dramCapacity) {
        std::string msg = "DRAM capacity is less than " +
                          std::to_string(minMergeFanIn + minMergeFanOut) + " SSD pages";
        throw std::runtime_error(msg);
    }
    if (minMergeFanOut * _ssdPageSize > _dram->getMergeFanOutRecords()) {
        std::string msg =
            "DRAM merge fan-in is less than " + std::to_string(minMergeFanIn) + " SSD pages";
        throw std::runtime_error(msg);
    }
    if (minMergeFanIn * _ssdPageSize > _dram->getMergeFanInRecords()) {
        std::string msg =
            "DRAM merge fan-in is less than " + std::to_string(minMergeFanIn) + " SSD pages";
        throw std::runtime_error(msg);
    }
    if ((minMergeFanIn + minMergeFanOut) * _hddPageSize > _ssdCapacity) {
        std::string msg = "SSD capacity is less than " +
                          std::to_string(minMergeFanIn + minMergeFanOut) + " HDD pages";
        throw std::runtime_error(msg);
    }
    if (minMergeFanOut * _hddPageSize > _ssd->getMergeFanOutRecords()) {
        std::string msg =
            "SSD merge fan-in is less than " + std::to_string(minMergeFanOut) + " HDD pages";
        throw std::runtime_error(msg);
    }
    if (minMergeFanIn * _hddPageSize > _ssd->getMergeFanInRecords()) {
        std::string msg =
            "SSD merge fan-in is less than " + std::to_string(minMergeFanIn) + " HDD pages";
        throw std::runtime_error(msg);
    }
    printvv("Init done\n");
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
    printv("deleted SSD\n");
    flushv();
    HDD::deleteInstance();
    printv("deleted HDD\n");
    flushv();
    printvv("Cleanup done\n");
    flushvv();
}


int main(int argc, char *argv[]) {

    // Read command line arguments
    readCmdlineArgs(argc, argv);

    // Validate the input and initialize the system
    init();


    if (!Config::VERIFY_ONLY) {
        Plan *scanPlan = new ScanPlan(Config::NUM_RECORDS, Config::INPUT_FILE);
        Plan *const plan = new SortPlan(scanPlan);

        Iterator *const it = plan->init();
        it->run();

        delete it;
        delete plan;

        // Cleanup what was initialized in init()
        cleanup();
    }

    if (Config::VERIFY_ONLY || Config::VERIFY) {
        uint64_t capacityMB = 1024; // 1 GB or 1024 memory used for verification

        // Verify the order
        auto start = std::chrono::steady_clock::now();
        verifyOrder(Config::OUTPUT_FILE, capacityMB);
        auto end = std::chrono::steady_clock::now();
        auto dur = std::chrono::duration_cast<std::chrono::seconds>(end - start);
        printvv("Order Verification Duration %lld seconds / %lld minutes\n", dur.count(),
                dur.count() / 60);

        // Verify the integrity
        auto startIntegrity = std::chrono::steady_clock::now();
        verifyIntegrity(Config::INPUT_FILE, Config::OUTPUT_FILE, capacityMB);
        auto endIntegrity = std::chrono::steady_clock::now();
        dur = std::chrono::duration_cast<std::chrono::seconds>(endIntegrity - startIntegrity);
        printvv("Integrity Verification Duration %lld seconds / %lld minutes\n", dur.count(),
                dur.count() / 60);
    }

    flushvv();

    return 0;
} // main
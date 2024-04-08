#include "config.h"
#include <cstdio>


// initialize static members with default values
int Config::RECORD_SIZE = 1024;
int Config::NUM_RECORDS = 20;
std::string Config::OUTPUT_FILE = "trace0.txt";
std::string Config::INPUT_FILE = "input.txt";

// implement print_config from config.h
void Config::print_config() {
    printf("---- Configuration: ----\n");
    printf("\tCACHE_SIZE: %d\n", CACHE_SIZE);
    printf("\tDRAM_SIZE: %d\n", DRAM_SIZE);
    printf("\tSSD_SIZE: %lld\n", SSD_SIZE);
    printf("\tSSD_LATENCY: %f\n", SSD_LATENCY);
    printf("\tSSD_BANDWIDTH: %d\n", SSD_BANDWIDTH);
    printf("\tHDD_SIZE: %d\n", HDD_SIZE);
    printf("\tHDD_LATENCY: %f\n", HDD_LATENCY);
    printf("\tHDD_BANDWIDTH: %d\n", HDD_BANDWIDTH);
    printf("\tRECORD_KEY_SIZE: %d\n", RECORD_KEY_SIZE);
    printf("\tRECORD_SIZE: %d\n", Config::RECORD_SIZE);
    printf("\tNUM_RECORDS: %d\n", NUM_RECORDS);
    printf("\tOUTPUT_FILE: %s\n", OUTPUT_FILE.c_str());
    printf("\tINPUT_FILE: %s\n", INPUT_FILE.c_str());
    printf("---- ---- ----\n");
}
#include "config.h"
#include <cstdio>


// initialize static members with default values
int Config::RECORD_SIZE = 1024;
int Config::NUM_RECORDS = 20;
std::string Config::OUTPUT_FILE = "output.txt";
std::string Config::INPUT_FILE = "input.txt";
std::string Config::TRACE_FILE = "trace.txt";

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
    printf("\tTRACE_FILE: %s\n", TRACE_FILE.c_str());
    printf("---- ---- ----\n");
}


std::ofstream Logger::traceFile;

void Logger::init(const std::string &filename) { traceFile.open(filename); }

void Logger::write(const std::string &message) {
    if (traceFile.is_open()) {
        traceFile << message << std::endl;
    }
}

void Logger::writef(const char *format, ...) {
    if (traceFile.is_open()) {
        char buffer[1024];
        va_list args;
        va_start(args, format);
        vsnprintf(buffer, sizeof(buffer), format, args);
        va_end(args);
        traceFile << buffer << std::endl;
    }
}

void Logger::close() {
    if (traceFile.is_open()) {
        traceFile.close();
    }
}

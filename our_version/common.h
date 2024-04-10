#ifndef _COMMON_H_
#define _COMMON_H_

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


// =========================================================
// ------------------------- Config ------------------------
// =========================================================


class Config {
  public:
    // variables
    static constexpr int CACHE_SIZE = 1 * 1 * 1024;           // 1 KB
    static constexpr int DRAM_SIZE = 1 * 10 * 1024;           // 100 MB
    static constexpr double DRAM_LATENCY = 1.0 / (10 * 1000); // 0.1 ms
    static constexpr int DRAM_BANDWIDTH = 100 * 1024 * 1024;  // 100 MB/s
    static constexpr int DRAM_BUFFER_SIZE =
        DRAM_BANDWIDTH * DRAM_LATENCY;                               // 10 MB
    static constexpr long long SSD_SIZE = 10LL * 1024 * 1024 * 1024; // 10 GB
    static constexpr double SSD_LATENCY = 1.0 / (10 * 1000);         // 0.1 ms
    static constexpr int SSD_BANDWIDTH = 100 * 1024 * 1024;          // 100 MB/s
    static constexpr int HDD_SIZE = INT_MAX;                         // Infinite
    static constexpr double HDD_LATENCY = 1 * 10.0 / 1000;           // 10 ms
    static constexpr int HDD_BANDWIDTH = 100 * 1024 * 1024;          // 100 MB/s
    static constexpr int RECORD_KEY_SIZE = 8;                        // 8 bytes
    static int RECORD_SIZE; // 1024 bytes
    static int NUM_RECORDS; // 20 records
    static std::string OUTPUT_FILE;
    static std::string INPUT_FILE;
    static std::string TRACE_FILE;

}; // class Config

void printConfig();


// =========================================================
// ------------------------- Record ------------------------
// =========================================================


class Record {
  public:
    char *data;
    Record() { data = new char[Config::RECORD_SIZE]; }
    Record(char *data) : data(data) {}
    ~Record() { delete[] data; }
    // default comparison based on first 8 bytes of data
    bool operator<(const Record &other) const {
        return std::strncmp(data, other.data, Config::RECORD_KEY_SIZE) < 0;
    }
}; // class Record


// =========================================================
// ------------------------- Logger ------------------------
// =========================================================


// singleton logger class
class Logger {
  private:
    std::ofstream traceFile;
    Logger() {
        if (!traceFile.is_open())
            traceFile.open(Config::TRACE_FILE);
    }

  public:
    static Logger *getInstance() {
        static Logger instance;
        return &instance;
    }

    void write(const std::string &message);
    void writef(const char *format, ...);
    ~Logger() { traceFile.close(); }
};


// =========================================================
// ------------------------- Assert ------------------------
// =========================================================


void Assert(bool const predicate, char const *const file, int const line,
            char const *const function);

#if defined(_DEBUG) || defined(DEBUG)
#define DebugAssert(b) Assert((b), __FILE__, __LINE__, __FUNCTION__)
#else // _DEBUG DEBUG
#define DebugAssert(b) (void)(0)
#endif // _DEBUG DEBUG


#ifdef USE_LOGFILE
std::ofstream logFile("log.txt");
#endif // USE_LOGFILE

void printVerbose(char const *const file, int const line,
                  char const *const function, const char *format, ...);
#define printv(...) printVerbose(__FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)


#endif // _COMMON_H_
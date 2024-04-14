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
#include <sstream>
#include <string>
#include <vector>


// =========================================================
// ------------------------- Config ------------------------
// =========================================================


class Config {
  public:
    // variables

    // ---- Cache ----
    static int CACHE_SIZE; // 1 KB
    // ---- DRAM ----
    static int DRAM_SIZE;        // 100 MB
    static double DRAM_LATENCY;  // 0.1 ms
    static int DRAM_BANDWIDTH;   // 100 MB/s
    static int DRAM_BUFFER_SIZE; // 10 MB
    // ---- SSD ----
    static long long SSD_SIZE; // 10 GB
    static double SSD_LATENCY; // 0.1 ms
    static int SSD_BANDWIDTH;  // 100 MB/s
    // ---- HDD ----
    static int HDD_SIZE;       // Infinite
    static double HDD_LATENCY; // 10 ms
    static int HDD_BANDWIDTH;  // 100 MB/s
    // ---- Record ----
    static int RECORD_KEY_SIZE; // 8 bytes
    static int RECORD_SIZE;     // 1024 bytes
    static int NUM_RECORDS;     // 20 records
    // ---- File ----
    static std::string OUTPUT_FILE;
    static std::string INPUT_FILE;
    static std::string TRACE_FILE;
    // ---- useful for sorting ----
    static int N_RECORDS_IN_CACHE;
    static int N_RECORDS_IN_DRAM;
    static int N_RECORDS_IN_SSD;
    static int N_RECORDS_IN_DRAM_BUFFER;
}; // class Config

void printConfig();
void readConfig(const std::string &configFile);
void calcConfig();


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

#if defined(_DEBUG)
#define DebugAssert(b) Assert((b), __FILE__, __LINE__, __FUNCTION__)
#else // _DEBUG
#define DebugAssert(b) (void)(0)
#endif // _DEBUG

extern std::ofstream logFile;

void printVerbose(bool vv, char const *const file, int const line,
                  char const *const function, const char *format, ...);
#define printv(...)                                                            \
    printVerbose(false, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define printvv(...)                                                           \
    printVerbose(true, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)

#endif // _COMMON_H_
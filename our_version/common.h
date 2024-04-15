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


#define ROUNDUP_4K(x) (((x) + 4095) & ~4095)


// =========================================================
// ------------------------- Config ------------------------
// =========================================================


class Config {
  public:
    // variables

    // ---- Cache ----
    static int CACHE_SIZE; // 1 KB
    // ---- DRAM ----
    static long long DRAM_SIZE; // 100 MB
    static double DRAM_LATENCY; // 0.1 ms
    static int DRAM_BANDWIDTH;  // 100 MB/s
    // ---- SSD ----
    static long long SSD_SIZE; // 10 GB
    static double SSD_LATENCY; // 0.1 ms
    static int SSD_BANDWIDTH;  // 100 MB/s
    // ---- HDD ----
    static long long HDD_SIZE; // Infinite
    static double HDD_LATENCY; // 10 ms
    static int HDD_BANDWIDTH;  // 100 MB/s
    // ---- Record ----
    static int RECORD_KEY_SIZE;   // 8 bytes
    static int RECORD_SIZE;       // 1024 bytes
    static long long NUM_RECORDS; // 20 records
    // ---- File ----
    static std::string OUTPUT_FILE;
    static std::string INPUT_FILE;
    static std::string TRACE_FILE;
}; // class Config

void printConfig();
void readConfig(const std::string &configFile);
long long getInputSizeInBytes();
long long getInputSizeInGB();
std::string formatNum(long long num);

// =========================================================
// ------------------------- Record ------------------------
// =========================================================


class Record {
  public:
    char *data;
    Record() {}
    Record(char *data) : data(data) {}
    // ~Record() { delete[] data; }
    // default comparison based on first 8 bytes of data
    bool operator<(const Record &other) const {
        return std::strncmp(data, other.data, Config::RECORD_KEY_SIZE) < 0;
    }
    // to string
    std::string toString() const {
        std::ostringstream oss;
        oss << "Record(" << std::string(data, Config::RECORD_SIZE) << ")";
        return oss.str();
    }
    /**
     * @brief Check if the record is valid; if all characters are alphanumeric
     */
    bool isValid();
    void invalidate();
}; // class Record

std::string recordToString(char *data);

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


void flushVerbose();
#define flushv() flushVerbose()

#endif // _COMMON_H_
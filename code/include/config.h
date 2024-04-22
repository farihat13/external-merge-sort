#ifndef _CONFIG_H_
#define _CONFIG_H_


#include "types.h"
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


#define ROUNDUP(x, byY) (((x) + (byY - 1)) & ~(byY - 1))
#define ROUNDDOWN(x, byY) ((x) & ~(byY - 1))
#define ROUNDUP_4K(x) (((x) + 4095) & ~4095)
#define BYTE_TO_KB(x) ((x) / 1024)
#define BYTE_TO_MB(x) ((x) / (1024 * 1024))
#define BYTE_TO_GB(x) ((x) / (1024 * 1024 * 1024))
#define MB_TO_BYTE(x) ((x) * (1024 * 1024))
#define GB_TO_BYTE(x) ((x) * (1024 * 1024 * 1024))

#define SEC_TO_MS(x) ((x) * 1000)
#define MS_TO_SEC(x) ((x) / 1000)


// =========================================================
// ------------------------- Config ------------------------
// =========================================================


class Config {
  public:
    // variables

    // ---- Cache ----
    static int CACHE_SIZE; // 1 KB
    // ---- DRAM ----
    static ByteCount DRAM_CAPACITY; // 100 MB
    static double DRAM_LATENCY;     // 0.1 ms
    static int DRAM_BANDWIDTH;      // 100 MB/s
    // ---- SSD ----
    static ByteCount SSD_CAPACITY; // 10 GB
    static double SSD_LATENCY;     // 0.1 ms
    static int SSD_BANDWIDTH;      // 100 MB/s
    // ---- HDD ----
    static ByteCount HDD_CAPACITY; // Infinite
    static double HDD_LATENCY;     // 10 ms
    static int HDD_BANDWIDTH;      // 100 MB/s
    // ---- Record ----
    static int RECORD_KEY_SIZE;  // 8 bytes
    static int RECORD_SIZE;      // 1024 bytes
    static RowCount NUM_RECORDS; // 20 records
    // ---- File ----
    static std::string OUTPUT_FILE;
    static std::string INPUT_FILE;
    static std::string TRACE_FILE;
}; // class Config

void printConfig();
void readConfig(const std::string &configFile);
ByteCount getInputSizeInBytes();
ByteCount getInputSizeInMB();
ByteCount getInputSizeInGB();
std::string formatNum(uint64_t num);


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


extern std::ofstream logFile;

void printVerbose(bool vv, char const *const file, int const line, char const *const function,
                  const char *format, ...);
#define printv(...) printVerbose(false, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define printvv(...) printVerbose(true, __FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)


void flushVerbose();
#define flushv() flushVerbose()


// =========================================================
// ----------------------- Utilities -----------------------
// =========================================================


ByteCount getFileSize(const std::string &filename);
std::string getSizeDetails(ByteCount size);


#endif // _CONFIG_H_
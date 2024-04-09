#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <cstring>
#include <cstdio>
#include <string>
#include <cstdarg>
#include <climits>
#include <cassert>



class Config {
  public:
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

    static void print_config() {
        std::cout << "RECORD_SIZE: " << RECORD_SIZE << std::endl;
        std::cout << "NUM_RECORDS: " << NUM_RECORDS << std::endl;
        std::cout << "TRACE_FILE: " << TRACE_FILE << std::endl;
    }
}; // class Config


int Config::RECORD_SIZE = 1024;
int Config::NUM_RECORDS = 20;
std::string Config::OUTPUT_FILE = "output.txt";
std::string Config::INPUT_FILE = "input.txt";
std::string Config::TRACE_FILE = "trace";


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


class Logger {
  public:
    static void init(const std::string &filename) { traceFile.open(filename); }

    static void write(const std::string &message) { traceFile << message; }

    static void writef(const char *format, ...) {
        va_list args;
        va_start(args, format);
        char buffer[1024];
        vsnprintf(buffer, sizeof(buffer), format, args);
        va_end(args);
        traceFile << buffer;
    }

    static void close() { traceFile.close(); }

  private:
    static std::ofstream traceFile;
}; // class Logger
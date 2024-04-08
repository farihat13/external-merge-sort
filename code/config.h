#ifndef CONFIG_H
#define CONFIG_H


#include <climits>
#include <string>


class Config {
  public:
    static constexpr int CACHE_SIZE = 1 * 1024 * 1024;               // 1 MB
    static constexpr int DRAM_SIZE = 100 * 1024 * 1024;              // 100 MB
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

    static void print_config();
}; // class Config


#endif // CONFIG_H
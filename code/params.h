#pragma once

#include "defs.h"
#include <climits>
#include <sstream>
#include <string>


namespace Config {
constexpr int CACHE_SIZE = 1 * 1024 * 1024;               // 1 MB
constexpr int DRAM_SIZE = 100 * 1024 * 1024;              // 100 MB
constexpr long long SSD_SIZE = 10LL * 1024 * 1024 * 1024; // 10 GB
constexpr double SSD_LATENCY = 1.0 / (10 * 1000);         // 0.1 ms
constexpr int SSD_BANDWIDTH = 100 * 1024 * 1024;          // 100 MB/s
constexpr int HDD_SIZE = INT_MAX;                         // Infinite
constexpr double HDD_LATENCY = 1 * 10.0 / 1000;           // 10 ms
constexpr int HDD_BANDWIDTH = 100 * 1024 * 1024;          // 100 MB/s
constexpr int RECORD_KEY_SIZE = 8;                        // 8 bytes

std::string configToString() {
    std::stringstream ss;
    ss << "Cache Size: " << CACHE_SIZE << " bytes\n";
    ss << "DRAM Size: " << DRAM_SIZE << " bytes\n";
    ss << "SSD Size: " << SSD_SIZE << " bytes\n";
    ss << "SSD Latency: " << SSD_LATENCY << " seconds\n";
    ss << "SSD Bandwidth: " << SSD_BANDWIDTH << " bytes/s\n";
    ss << "HDD Size: "
       << (HDD_SIZE == INT_MAX ? "Infinite" : std::to_string(HDD_SIZE))
       << " bytes\n";
    ss << "HDD Latency: " << HDD_LATENCY << " seconds\n";
    ss << "HDD Bandwidth: " << HDD_BANDWIDTH << " bytes/s\n";
    return ss.str();
}
} // namespace Config


class Input {
  public:
    Input(int num_records, int record_size, std::string output_file)
        : num_records(num_records), record_size(record_size),
          output_file(output_file) {}
    ~Input() {}

    // to string
    std::string toString() {
        std::stringstream ss;
        ss << "Number of Records: " << num_records << "\n";
        ss << "Record Size: " << record_size << " bytes\n";
        ss << "Output File: " << output_file << "\n";
        return ss.str();
    }

  private:
    int num_records;
    int record_size;
    std::string output_file;
};

#include "config.h"
#include <iomanip>
#include <sys/stat.h>

// =========================================================
// ------------------------- Config ------------------------
// =========================================================

// ==== Default Configurations ====
// ---- Cache ----
int Config::CACHE_SIZE = 1 * 1024 * 1024; // 1 MB
// ---- DRAM ----
// ByteCount Config::DRAM_CAPACITY = 1LL * 20 * 1024 * 1024; // 20 MB TODO: change to 100 MB
ByteCount Config::DRAM_CAPACITY = 1LL * 100 * 1024 * 1024; // 100 MB
double Config::DRAM_LATENCY = 1.0 / (1000 * 1000);         // 10 microsecond
int Config::DRAM_BANDWIDTH = 100 * 1024 * 1024;            // 100 GB/s
// DRAM buffer size = 1 MB
// ---- SSD ----
// ByteCount Config::SSD_CAPACITY = 1LL * 75 * 1024 * 1024; // 75 MB TODO: change to 10GB
ByteCount Config::SSD_CAPACITY = 10LL * 1024 * 1024 * 1024; // 10GB
double Config::SSD_LATENCY = 1.0 / (10 * 1000);             // 0.1 ms
int Config::SSD_BANDWIDTH = 100 * 1024 * 1024;              // 100 MB/s
// SSD buffer size = 10 MB
// ---- HDD ----
ByteCount Config::HDD_CAPACITY = INFINITE_CAPACITY * 1LL; // Infinite
double Config::HDD_LATENCY = 1 * 10.0 / 1000;             // 10 ms
int Config::HDD_BANDWIDTH = 100 * 1024 * 1024;            // 100 MB/s
// ---- Record ----
int Config::RECORD_KEY_SIZE = 8;     // 8 bytes
int Config::RECORD_SIZE = 1024;      // 1024 bytes
RowCount Config::NUM_RECORDS = 20LL; // 20 records
// ---- Verify ----
std::string Config::VERIFY_INPUTDIR = "Verify_parts/input/";
std::string Config::VERIFY_OUTPUTDIR = "Verify_parts/output/";
int Config::VERIFY_HASH_BYTES = sizeof(uint64_t);
// ---- File ----
std::string Config::OUTPUT_FILE = "output.txt";
std::string Config::INPUT_FILE = "input.txt";
std::string Config::TRACE_FILE = "trace.log";
// ================================

void printConfig() {
    printv("\n== Configurations ==\n");
    // ---- Cache ----
    printv("\tCACHE_SIZE: %d bytes\n", Config::CACHE_SIZE);
    // ---- DRAM ----
    printv("\tDRAM_SIZE: %d bytes\n", Config::DRAM_CAPACITY);
    printv("\tDRAM_LATENCY: %f\n", Config::DRAM_LATENCY);
    printv("\tDRAM_BANDWIDTH: %d\n", Config::DRAM_BANDWIDTH);
    // ---- SSD ----
    printv("\tSSD_SIZE: %lld bytes\n", Config::SSD_CAPACITY);
    printv("\tSSD_LATENCY: %f\n", Config::SSD_LATENCY);
    printv("\tSSD_BANDWIDTH: %d\n", Config::SSD_BANDWIDTH);
    // ---- HDD ----
    printv("\tHDD_SIZE: %d bytes\n", Config::HDD_CAPACITY);
    printv("\tHDD_LATENCY: %f\n", Config::HDD_LATENCY);
    printv("\tHDD_BANDWIDTH: %d\n", Config::HDD_BANDWIDTH);
    // ---- Record ----
    printv("\tRECORD_KEY_SIZE: %d bytes\n", Config::RECORD_KEY_SIZE);
    printv("\tRECORD_SIZE: %d bytes\n", Config::RECORD_SIZE);
    printv("\tNUM_RECORDS: %lld (%s)\n", Config::NUM_RECORDS,
           formatNum(Config::NUM_RECORDS).c_str());
    printv("\tInput Size: %sBytes\n", formatNum(getInputSizeInBytes()).c_str());
    // ---- File ----
    printv("\tOUTPUT_FILE: %s\n", Config::OUTPUT_FILE.c_str());
    printv("\tINPUT_FILE: %s\n", Config::INPUT_FILE.c_str());
    printv("\tTRACE_FILE: %s\n", Config::TRACE_FILE.c_str());
    printv("== End Configurations ==\n\n");
}

void readConfig(const std::string &filename) {
    std::ifstream configFile(filename);
    std::string line;
    while (getline(configFile, line)) {
        std::istringstream is_line(line);
        std::string key;
        if (getline(is_line, key, '=')) {
            std::string value;
            if (getline(is_line, value)) {
                // Trim possible whitespace from the value
                value.erase(0, value.find_first_not_of(" \t"));
                value.erase(value.find_last_not_of(" \t") + 1);

                if (key == "CACHE_SIZE")
                    Config::CACHE_SIZE = stoi(value);
                else if (key == "DRAM_SIZE")
                    Config::DRAM_CAPACITY = stoll(value);
                else if (key == "DRAM_LATENCY")
                    Config::DRAM_LATENCY = stod(value);
                else if (key == "DRAM_BANDWIDTH")
                    Config::DRAM_BANDWIDTH = stoi(value);
                else if (key == "SSD_SIZE")
                    Config::SSD_CAPACITY = stoll(value);
                else if (key == "SSD_LATENCY")
                    Config::SSD_LATENCY = stod(value);
                else if (key == "SSD_BANDWIDTH")
                    Config::SSD_BANDWIDTH = stoi(value);
                else if (key == "HDD_SIZE")
                    Config::HDD_CAPACITY = stoll(value);
                else if (key == "HDD_LATENCY")
                    Config::HDD_LATENCY = stod(value);
                else if (key == "HDD_BANDWIDTH")
                    Config::HDD_BANDWIDTH = stoi(value);
                else if (key == "RECORD_KEY_SIZE")
                    Config::RECORD_KEY_SIZE = stoi(value);
                else if (key == "RECORD_SIZE")
                    Config::RECORD_SIZE = stoi(value);
                else if (key == "NUM_RECORDS")
                    Config::NUM_RECORDS = stoll(value);
                else if (key == "OUTPUT_FILE")
                    Config::OUTPUT_FILE = value;
                else if (key == "INPUT_FILE")
                    Config::INPUT_FILE = value;
                else if (key == "TRACE_FILE")
                    Config::TRACE_FILE = value;
            }
        }
    }
    configFile.close();
}

ByteCount getInputSizeInBytes() { return Config::NUM_RECORDS * Config::RECORD_SIZE; }
ByteCount getInputSizeInMB() { return getInputSizeInBytes() / (1024 * 1024); }
ByteCount getInputSizeInGB() { return getInputSizeInBytes() / (1024 * 1024 * 1024); }

std::string formatNum(uint64_t number) {
    bool isNegative = number < 0;
    if (isNegative)
        number = -number;

    std::string suffix = "";
    double displayNumber = number;

    if (number >= 1000000) {
        displayNumber = number / 1000000.0;
        suffix = " M";
    } else if (number >= 1000) {
        displayNumber = number / 1000.0;
        suffix = " K";
    }

    // Convert the number to a formatted string with one decimal place
    std::ostringstream stream;
    stream << std::fixed << std::setprecision(1) << displayNumber;
    std::string result = stream.str();

    // Remove trailing zeros and possibly the decimal point
    size_t dotPos = result.find('.');
    if (dotPos != std::string::npos) {
        // Remove trailing zeros
        while (result.back() == '0') {
            result.pop_back();
        }
        // If the last character is the dot, remove it
        if (result.back() == '.') {
            result.pop_back();
        }
    }

    return (isNegative ? "-" : "") + result + suffix;
}


// =========================================================
// ------------------------- Logger ------------------------
// =========================================================

void Logger::write(const std::string &message) {
    if (!traceFile.is_open()) {
        std::cout << "Error: Trace file not open" << std::endl;
        return;
    }
    traceFile << message;
}

void Logger::writef(const char *format, ...) {
    if (!traceFile.is_open()) {
        std::cout << "Error: Trace file not open" << std::endl;
        return;
    }
    va_list args;
    va_start(args, format);
    char buffer[1024];
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);
    traceFile << buffer;
}

// =========================================================
// ------------------------- Assert ------------------------
// =========================================================

void Assert(bool const predicate, char const *const file, int const line,
            char const *const function) {
    if (predicate)
        return;

    fflush(stdout);
    fprintf(stderr, "failed assertion at %s:%d:%s\n", file, line, function);
    fflush(stderr);

#if 0
	assert (false);
#else
    exit(1);
#endif
} // Assert


std::ofstream logFile;

void printVerbose(bool vv, char const *const file, int const line, char const *const function,
                  const char *format, ...) {
    va_list args;
    va_start(args, format);

#if defined(_USE_LOGFILE)
    if (!logFile.is_open()) {
        logFile.open("log.txt");
    }
    if (vv)
        logFile << file << ":" << line << ":" << function << " ";
    char buffer[1024];
    vsnprintf(buffer, sizeof(buffer), format, args);
    logFile << buffer;
#else
    if (vv)
        std::printf("%s:%d:%s\t", file, line, function);
    std::vprintf(format, args);
#endif

    va_end(args);
}


void flushVerbose() {
#if defined(_USE_LOGFILE)
    if (logFile.is_open())
        logFile.flush();
#else
    std::fflush(stdout);
#endif
}


// =========================================================
// ----------------------- Utilities -----------------------
// =========================================================

ByteCount getFileSize(const std::string &filename) {
    struct stat stat_buf = {0};
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

std::string getSizeDetails(ByteCount bytes) {
    std::ostringstream ss;
    auto nRecords = bytes / Config::RECORD_SIZE;
    auto kilobytes = BYTE_TO_KB(bytes);
    auto megabytes = BYTE_TO_MB(bytes);
    ss << "Size: " << nRecords << " records / " << bytes << " bytes / " << kilobytes << " KB / "
       << megabytes << " MB";
    return ss.str();
}
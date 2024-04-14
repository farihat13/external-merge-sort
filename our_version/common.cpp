#include "common.h"


// =========================================================
// ------------------------- Config ------------------------
// =========================================================

int Config::RECORD_SIZE = 1024;
int Config::NUM_RECORDS = 20;
std::string Config::OUTPUT_FILE = "output.txt";
std::string Config::INPUT_FILE = "input.txt";
std::string Config::TRACE_FILE = "trace";

void printConfig() {
    std::cout << "== Configurations ==\n";
    std::cout << "\tRECORD_SIZE: " << Config::RECORD_SIZE << std::endl;
    std::cout << "\tNUM_RECORDS: " << Config::NUM_RECORDS << std::endl;
    std::cout << "\tTRACE_FILE: " << Config::TRACE_FILE << std::endl;
    std::cout << "== End Configurations ==\n";
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

void printVerbose(char const *const file, int const line,
                  char const *const function, const char *format, ...) {
    va_list args;
    va_start(args, format);

#if defined(_USE_LOGFILE)
    if (!logFile.is_open()) {
        logFile.open("log.txt");
    }
    logFile << file << ":" << line << ":" << function << " ";
    char buffer[1024];
    vsnprintf(buffer, sizeof(buffer), format, args);
    logFile << buffer;
#else
    std::printf("%s:%d:%s\t", file, line, function);
    std::vprintf(format, args);
#endif

    va_end(args);
}
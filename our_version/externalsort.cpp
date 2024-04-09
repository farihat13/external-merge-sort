#include "externalsort.h"


void readCmdlineArgs(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr,
                "Usage: %s -c <num_records> -s <record_size> -o <trace_file>\n",
                argv[0]);
        exit(1);
    }

    int num_records = 0;
    int record_size = 0;
    std::string trace_file = "";
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-c") == 0) {
            if (i + 1 < argc) {
                num_records = std::atoi(argv[++i]);
            } else {
                fprintf(stderr, "Option -c requires an argument.\n");
                exit(1);
            }
        } else if (strcmp(argv[i], "-s") == 0) {
            if (i + 1 < argc) {
                record_size = std::atoi(argv[++i]);
            } else {
                fprintf(stderr, "Option -s requires an argument.\n");
                exit(1);
            }
        } else if (strcmp(argv[i], "-o") == 0) {
            if (i + 1 < argc) {
                trace_file = argv[++i];
            } else {
                fprintf(stderr, "Option -o requires an argument.\n");
                exit(1);
            }
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            exit(1);
        }
    }
    Config::RECORD_SIZE = record_size;
    Config::NUM_RECORDS = num_records;
    Config::TRACE_FILE = trace_file;

} // readCmdlineArgs


void gen_a_record(char *s, const int len) {
    static const char alphanum[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
} // gen_a_record

void generateInputFile(const std::string &filename, int recordSize,
                       int numRecords) {
    std::ofstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Error opening file for writing." << std::endl;
        exit(1);
    }
    srand(time(nullptr)); // Seed random number generator
    char *record = new char[recordSize];
    for (int i = 0; i < numRecords; ++i) {
        gen_a_record(record, Config::RECORD_SIZE);
        file.write(record, Config::RECORD_SIZE);
        // file.write("\n", 1);
    }
    file.close();
    printf("Generated %d records in file %s\n", numRecords, filename.c_str());
}

void quickSort(Record *records, int n) { // In-memory
    printf("Sorting %d records\n", n);
    // if (n <= 1)
    //     return;
    // Record pivot = records[n / 2];
    // Record *left = records;
    // Record *right = records + n - 1;
    // while (left <= right) {
    //     if (*left < pivot) {
    //         left++;
    //         continue;
    //     }
    //     if (pivot < *right) {
    //         right--;
    //         continue;
    //     }
    //     std::swap(*left, *right);
    //     left++;
    //     right--;
    // }
    // quickSort(records, right - records + 1);
    // quickSort(left, records + n - left);
}

void externalSort(const std::string &inputFile, const std::string &outputFile,
                  int recordSize) {
    std::ifstream file(inputFile, std::ios::binary);
    if (!file) {
        std::cerr << "Error opening input file." << std::endl;
        exit(1);
    }

    // Read records into memory and sort cache-size chunks
    int n_cache_in_dram = Config::DRAM_SIZE / Config::CACHE_SIZE;
    int n_records_in_cache = Config::CACHE_SIZE / Config::RECORD_SIZE;
    printf("n_cache_in_dram: %d\n", n_cache_in_dram);
    printf("n_records_in_cache: %d\n", n_records_in_cache);
    printf("each record size: %lu bytes\n", sizeof(Record));
    int i = 0;
    Record *dram = new Record[n_cache_in_dram * n_records_in_cache];
    
    std::ofstream cacheFile("cache.tmp", std::ios::binary);
    while (true) {
        if (!file.read(dram[i++].data, Config::RECORD_SIZE)) {
            for (int j = 0; j < n_cache_in_dram; j++) {
                if (j * n_records_in_cache >= i)
                    break;
                int len = i - j * n_records_in_cache;
                len = std::min(len, n_records_in_cache);
                if (j * (n_records_in_cache + 1) < i)
                    quickSort(dram + j * n_records_in_cache, len);
            }
            for (i = 0; i < n_cache_in_dram * n_records_in_cache; i++) {
                cacheFile.write(dram[i].data, Config::RECORD_SIZE);
                cacheFile.write("\n", 1);
            }
            cacheFile.write("\n", 1);
            printf("Sorted cache-size chunk\n");
        }
        printf("Read record %d\n", i);
        if (i == n_cache_in_dram * n_records_in_cache) {
            for (int j = 0; j < n_cache_in_dram; j++) {
                quickSort(dram + j * n_records_in_cache, n_records_in_cache);
                printf("Sorted record [%d, %d)\n", j * n_records_in_cache,
                       (j + 1) * n_records_in_cache);
            }
            for (i = 0; i < n_cache_in_dram * n_records_in_cache; i++) {
                cacheFile.write(dram[i].data, Config::RECORD_SIZE);
                cacheFile.write("\n", 1);
            }
            cacheFile.write("\n", 1);
            printf("Sorted cache-size chunk\n");
            i = 0;
        }
    }
    cacheFile.close();
    file.close();
    printf("Sorted cache-size chunks\n");

    // // Write sorted records to output file
    // std::ofstream outputFileStream(outputFile, std::ios::binary);
    // if (!outputFileStream) {
    //     std::cerr << "Error opening output file." << std::endl;
    //     exit(1);
    // }
    // for (const auto &record : records) {
    //     outputFileStream.write(reinterpret_cast<const char *>(&record.key),
    //                            sizeof(record.key));
    //     outputFileStream.write(record.data.data(), record.data.size());
    // }

    // outputFileStream.close();
}

int main(int argc, char *argv[]) {
    readCmdlineArgs(argc, argv);

    generateInputFile(Config::INPUT_FILE, Config::RECORD_SIZE,
                      Config::NUM_RECORDS);
    externalSort(Config::INPUT_FILE, Config::OUTPUT_FILE, Config::RECORD_SIZE);

    return 0;
}

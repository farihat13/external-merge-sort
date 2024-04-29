#include "include/types.h"
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <thread>
#include <vector>

const RowCount NUM_RECORDS = 1024 * 1024 * 10; // 10 million records
const ByteCount RECORD_SIZE = 2048;
const int NUM_THREADS = std::thread::hardware_concurrency();
const RowCount BATCH_SIZE = 4096 * 2; // Number of records per batch


void gen_a_record(char *s, const int len) {
    static const char alphanum[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
}

RowCount genInputBatch(const std::string &filename, const RowCount count) {
    std::string tmpfilename = filename + ".tmp";
    std::ofstream input_file(tmpfilename, std::ios::binary | std::ios::trunc);

    /**
     * generate records in batches
     */
    // calculate batch size and number of batches
    RowCount batchSize = 4096 * 2;
    batchSize = std::min(batchSize, count);
    if (batchSize % 2 != 0) { batchSize--; }
    RowCount nBatches = batchSize == 0 ? 0 : (count / batchSize);
    // allocate buffer for batch records
    char *buffer = new char[RECORD_SIZE * batchSize];
    RowCount n = 0;
    RowCount dup = 0;
    for (RowCount i = 0; i < nBatches; i++) {
        char *record = buffer;
        for (RowCount j = 0; j < batchSize; j++) {
            gen_a_record(record, RECORD_SIZE);
            record[RECORD_SIZE - 1] = '\n'; // TODO: remove later
            n++;
            record += RECORD_SIZE;
        }
        input_file.write(buffer, RECORD_SIZE * batchSize);
    }
    // free memory
    delete[] buffer;
    batchSize = count - n;
    buffer = new char[RECORD_SIZE * batchSize];
    if (batchSize > 0) {
        char *record = buffer;
        for (RowCount j = 0; j < batchSize; j++) {
            gen_a_record(record, RECORD_SIZE);
            record[RECORD_SIZE - 1] = '\n'; // TODO: remove later
            n++;
            record += RECORD_SIZE;
        }
        input_file.write(buffer, RECORD_SIZE * batchSize);
        // printv("%lld\n", n);
    }
    input_file.close();
    // rename the file
    rename(tmpfilename.c_str(), filename.c_str());
    // free memory
    delete[] buffer;
    // return
    return n;
}

// Function to generate random alphanumeric strings and write them to a file
void generate_and_write_batch(int thread_id, RowCount batch_start, RowCount batch_end) {
    const char alphanum[] = "0123456789"
                            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                            "abcdefghijklmnopqrstuvwxyz";

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, sizeof(alphanum) - 2);

    std::ostringstream filename;
    filename << "input_" << thread_id << ".tmp";
    std::ofstream file(filename.str(), std::ios::binary | std::ios::app);

    std::vector<char> buffer;
    buffer.reserve(RECORD_SIZE * (batch_end - batch_start));

    for (RowCount i = batch_start; i < batch_end; ++i) {
        for (ByteCount j = 0; j < RECORD_SIZE; ++j) {
            buffer.push_back(alphanum[dis(gen)]);
        }
        buffer[buffer.size() - 1] = '\n'; // TODO: remove later
    }

    file.write(buffer.data(), buffer.size());
    file.close();
}

// Function to merge multiple files into one
void merge_files(int num_files, const std::string &output_filename) {
    std::ofstream final_file(output_filename, std::ios::binary);

    for (int i = 0; i < num_files; ++i) {
        std::ostringstream filename;
        filename << "input_" << i << ".tmp";
        std::ifstream file(filename.str(), std::ios::binary);

        final_file << file.rdbuf();

        file.close();
        remove(filename.str().c_str()); // Optionally remove the temporary file
    }

    final_file.close();
}

int main() {

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    int total_batches = (NUM_RECORDS + BATCH_SIZE - 1) / BATCH_SIZE;

    for (int batch = 0; batch < total_batches; ++batch) {
        int batch_start = batch * BATCH_SIZE;
        int batch_end = std::min(batch_start + BATCH_SIZE, NUM_RECORDS);
        int thread_id = batch % NUM_THREADS; // Assign batch to a thread

        threads.emplace_back(generate_and_write_batch, thread_id, batch_start, batch_end);
    }

    // Wait for all threads to complete
    for (auto &thread : threads) {
        thread.join();
    }

    // Merge files
    merge_files(NUM_THREADS, "input.bin");

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "Time taken: " << elapsed.count() << " seconds" << std::endl;

    remove("input.bin");

    // Old code

    auto start_old = std::chrono::high_resolution_clock::now();
    genInputBatch("input_old.bin", NUM_RECORDS);
    auto end_old = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_old = end_old - start_old;
    std::cout << "Time taken (old): " << elapsed_old.count() << " seconds" << std::endl;

    remove("input_old.bin");

    return 0;
}

/**
 * Output:
 * Time taken: 151.708 seconds
 * Time taken (old): 399.647 seconds
 */
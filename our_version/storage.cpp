#include "storage.h"
#include <algorithm>
#include <vector>

// =========================================================
// -------------------- In-memory Quick Sort ---------------
// =========================================================

/**
 * @brief swap two records
 */
void swap(Record &a, Record &b) {
    // Record temp = a;
    // a = b;
    // b = temp;
    std::swap_ranges(a.data, a.data + Config::RECORD_SIZE, b.data);
}

/**
 * @brief partition the array using the last element as pivot
 */
int partition(Record *records, int low, int high) {
    Record pivot = records[high]; // choosing the last element as pivot
    int i = (low - 1);            // Index of smaller element

    for (int j = low; j <= high - 1; j++) {
        // If current element is smaller than the pivot, increment index of
        // smaller element and swap the elements
        if (records[j] < pivot) {
            i++;
            swap(records[i], records[j]);
        }
    }
    swap(records[i + 1], records[high]);
    return (i + 1);
}

void quickSortRecursive(Record *records, int low, int high) {
    if (low < high) {
        int pi = partition(records, low, high);
        quickSortRecursive(records, low, pi - 1);
        quickSortRecursive(records, pi + 1, high);
    }
}

void quickSort(Record *records, int nRecords) {
    quickSortRecursive(records, 0, nRecords - 1);
}

// =========================================================
// -------------------------- DRAM -------------------------
// =========================================================


DRAM *DRAM::instance = nullptr;

int DRAM::generateRun(std::ifstream &file, int nRecords) {

    if (nRecords > this->nRecords) {
        fprintf(stderr, "Error: DRAM read size exceeds DRAM capacity\n");
        exit(1);
    }
    file.read(this->data, nRecords * Config::RECORD_SIZE);
    int nBytesRead = file.gcount();
    nRecords = nBytesRead / Config::RECORD_SIZE;

    Record *records = new Record[nRecords];
    for (int i = 0; i < nRecords; i++) {
        records[i].data = this->data + i * Config::RECORD_SIZE;
    }
    quickSort(records, nRecords);
    printv("\tRead %d records (%d bytes) From Disk to DRAM and Sorted those\n",
           nRecords, nBytesRead);

#if defined(_DEBUG)
    for (int i = 0; i < nRecords; i++) {
        printv("\t\tRecord %2d: %s\n", i,
               recordToString(this->data + i * Config::RECORD_SIZE).c_str());
    }
#endif

    return nRecords;
}


// =========================================================
// -------------------------- SSD --------------------------
// =========================================================


SSD *SSD::instance = nullptr;


// ---------------------------------------------------------
// -------------------------- HDD --------------------------
// ---------------------------------------------------------

void HDD::externalSort() {

    std::ifstream file(Config::INPUT_FILE, std::ios::binary);
    if (!file) {
        std::cerr << "Error opening input file." << std::endl;
        exit(1);
    }

    // Read records into memory and sort cache-size chunks
    DRAM *dram = DRAM::getInstance();

    // generate DRAM size runs
    const int nRecordsInDRAM = Config::DRAM_SIZE / Config::RECORD_SIZE;
    int nRecordsReadFromHDD = 0;
    while (nRecordsReadFromHDD < Config::NUM_RECORDS) {
        int nRecords = dram->generateRun(file, nRecordsInDRAM);
        nRecordsReadFromHDD += nRecords;
        flushv();
    }
}

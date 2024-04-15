#include "storage.h"
#include <algorithm>
#include <vector>


// =========================================================
// ----------------------- Common Utils --------------------
// =========================================================

int readFile(std::string filename, int address, char *buffer, int nBytes) {
    std::ifstream file(filename, std::ios::binary);
    file.seekg(address, std::ios::beg);
    file.read(buffer, nBytes);
    file.close();
    // printvv("\tRead %d bytes from SSD\n", nBytes);
    return nBytes;
}

int writeFile(std::string filename, int address, char *buffer, int nBytes) {
    std::ofstream file(filename, std::ios::binary);
    file.seekp(address, std::ios::beg);
    file.write(buffer, nBytes);
    file.close();
    // printvv("\tWrote %d bytes to SSD\n", nBytes);
    return nBytes;
}


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

    if (nRecords > this->maxNumRecords) {
        fprintf(stderr, "Error: DRAM read size exceeds DRAM capacity\n");
        return -1;
    }

    // read nRecords from file to DRAM
    file.read(this->data, nRecords * Config::RECORD_SIZE);
    int nBytesRead = file.gcount();
    this->nRecords = nBytesRead / Config::RECORD_SIZE;

    // sort the records
    Record *records = new Record[nRecords];
    for (int i = 0; i < nRecords; i++) {
        records[i].data = this->data + i * Config::RECORD_SIZE;
    }
    quickSort(records, nRecords);

    printvv("\tRead %d records (%d bytes) From Disk to DRAM and Sorted those\n",
            nRecords, nBytesRead);

#if defined(_DEBUG)
    // for (int i = 0; i < nRecords; i++) {
    //     printv("\t\tRecord %2d: %s\n", i,
    //            recordToString(this->data + i * Config::RECORD_SIZE).c_str());
    // }
#endif

    return nRecords;
}


// =========================================================
// -------------------------- SSD --------------------------
// =========================================================


SSD *SSD::instance = nullptr;

int SSD::read(int address, char *buffer, int nBytes) {
    if (address + nBytes > this->maxNumRecords * Config::RECORD_SIZE) {
        fprintf(stderr, "Error: SSD read exceeds SSD capacity\n");
        return -1;
    }
    readFile(this->filename, address, buffer, nBytes);
    // printvv("\tRead %d bytes from SSD\n", nBytes);
    return nBytes;
}

int SSD::write(int address, char *buffer, int nBytes) {
    if (address + nBytes > this->maxNumRecords * Config::RECORD_SIZE) {
        fprintf(stderr, "Error: SSD write exceeds SSD capacity\n");
        return -1;
    }
    writeFile(this->filename, address, buffer, nBytes);
    // printvv("\tWrote %d bytes to SSD\n", nBytes);
    return nBytes;
}

int SSD::storeRun() {
    if (this->nRuns >= this->maxNumRuns) {
        fprintf(stderr, "Error: SSD is storing %d out of %d runs already\n",
                this->nRuns, this->maxNumRuns);
        return -1;
    }
    DRAM *dram = DRAM::getInstance();
    // copy the records from DRAM to SSD
    this->write(this->nRecords * Config::RECORD_SIZE, dram->data,
                dram->nRecords * Config::RECORD_SIZE);
    this->nRecords += dram->nRecords;
    this->nRuns++;
#if defined(_DEBUG)
    printvv("\tCopied %d records (%d bytes) from DRAM to SSD\n", dram->nRecords,
            dram->nRecords * Config::RECORD_SIZE);
    this->printState();
#endif
    return dram->nRecords;
}

int SSD::mergeRuns() {
    printv("STATE -> Merge the runs in SSD and store %d records in HDD\n",
           Config::NUM_RECORDS);
    // read the runs from SSD
    int nRuns = this->nRuns;
    int nRecords = this->nRecords;
    Record *records = new Record[nRecords];
    for (int i = 0; i < nRuns; i++) {
        this->read(i * Config::RECORD_SIZE, records[i].data,
                   Config::RECORD_SIZE);
    }

    // merge the runs
    std::vector<int> indexes(nRuns, 0);
    Record *output = new Record[nRecords];
    for (int i = 0; i < nRecords; i++) {
        int minIndex = -1;
        for (int j = 0; j < nRuns; j++) {
            if (indexes[j] < Config::RECORD_SIZE) {
                if (minIndex == -1) {
                    minIndex = j;
                } else if (records[j] < records[minIndex]) {
                    minIndex = j;
                }
            }
        }
        output[i] = records[minIndex];
        indexes[minIndex]++;
    }

    // write the merged runs to SSD
    for (int i = 0; i < nRecords; i++) {
        this->write(i * Config::RECORD_SIZE, output[i].data,
                    Config::RECORD_SIZE);
    }
    this->nRecords = nRecords;
    this->nRuns = 1;
    printv("\tMerged %d runs and stored %d records (%d bytes) in SSD\n", nRuns,
           nRecords, nRecords * Config::RECORD_SIZE);
    this->printState();
    return nRecords;
}

void SSD::mergeRunsPlan() {
    printv("STATE -> Merge the runs in SSD and store %d records in HDD\n",
           Config::NUM_RECORDS);
    // read the runs from SSD to memory
}


// ---------------------------------------------------------
// -------------------------- HDD --------------------------
// ---------------------------------------------------------

// HDD *HDD::instance = nullptr;


int HDD::read(int address, char *buffer, int nBytes) {
    readFile(this->filename, address, buffer, nBytes);
    return nBytes;
}

int HDD::write(int address, char *buffer, int nBytes) {
    writeFile(this->filename, address, buffer, nBytes);
    return nBytes;
}

void HDD::externalSort() {

    std::ifstream input(Config::INPUT_FILE, std::ios::binary);
    if (!input) {
        std::cerr << "Error opening input file." << std::endl;
        exit(1);
    }

    SSD *ssd = SSD::getInstance();

    // Read records into memory and sort cache-size chunks
    DRAM *dram = DRAM::getInstance();
    // generate DRAM size runs
    const int nRecordsInDRAM = Config::DRAM_SIZE / Config::RECORD_SIZE;
    int nRecordsReadFromHDD = 0;
    while (nRecordsReadFromHDD < Config::NUM_RECORDS) {
        // generate a DRAM_size_run
        int nRecords = dram->generateRun(input, nRecordsInDRAM);
        nRecordsReadFromHDD += nRecords;
        flushv();

        // store the run in SSD
        int nRecordsStoredInSSD = ssd->storeRun();
        if (nRecordsStoredInSSD == -1) {
            fprintf(stderr, "Error: SSD is full\n");
            exit(1);
        }
        if (nRecordsStoredInSSD == 0) {
            fprintf(stderr, "Error: No records stored in SSD\n");
            exit(1);
        }

        if (ssd->nRuns == ssd->maxNumRuns) {
            // sort the runs in SSD
            printv("Merge the runs in SSD and store %d records (%d bytes) in "
                   "HDD\n",
                   ssd->nRecords, ssd->nRecords * Config::RECORD_SIZE);
            // TODO: merge the runs in SSD
            ssd->mergeRuns();
            this->nRecords += ssd->nRecords;
            this->nRuns++;
            ssd->reset();
            this->printState();
            goto end;
        }
    }
    this->reset();

end:
    input.close();
}


void HDD::externalSortPlan() {

    SSD *ssd = SSD::getInstance();
    DRAM *dram = DRAM::getInstance();

    printv("\nExternal Sort Plan:\n");
    printv("Input size: %s bytes (%s records)\n",
           formatNum(getInputSizeInBytes()).c_str(),
           formatNum(Config::NUM_RECORDS).c_str());

    for (int i = 0; i < 1; i++) {
        printv("\nIteration %d:\n", i + 1);
        long long nRecordsReadFromHDD = 0;
        // generate DRAM size runs
        while (nRecordsReadFromHDD < Config::NUM_RECORDS) {

            // generate a DRAM_size_run
            dram->nRecords = std::min(
                dram->maxNumRecords, Config::NUM_RECORDS - nRecordsReadFromHDD);
            printv("\tACCESS -> read from HDD to DRAM: %s bytes (%d records)\n",
                   formatNum(dram->maxNumRecords * Config::RECORD_SIZE).c_str(),
                   dram->maxNumRecords);
            printv("\tSTATE -> Quick Sort in DRAM\n");
            flushv();
            nRecordsReadFromHDD += dram->nRecords;

            // store the run in SSD
            printv(
                "\tACCESS -> write from DRAM to SSD: %s bytes (%d records)\n",
                formatNum(dram->nRecords * Config::RECORD_SIZE).c_str(),
                dram->nRecords);
            ssd->nRecords += dram->nRecords;
            ssd->nRuns += 1;
            ssd->nRecordsPerRun = std::max(dram->nRecords, ssd->nRecordsPerRun);
            ssd->printState();

            if (ssd->nRuns == ssd->maxNumRuns) {
                // sort the runs in SSD
                // printv("Merge the runs in SSD and store %d records in HDD\n",
                //        Config::NUM_RECORDS);
                ssd->mergeRunsPlan();
            }

            // sort the runs in SSD
            printv("Merge the runs in SSD and store %d records in HDD\n",
                   Config::NUM_RECORDS);
        }
        // TODO:
        // this->nRecords += ssd->nRecords;
        // this->nRuns++;
        // this->nRecordsPerRun = ssd->nRecords;
        // ssd->reset();
        this->printState();
    }
}

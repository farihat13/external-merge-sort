#include "storage.h"
#include <algorithm>
#include <vector>


// =========================================================
// -------------------------- Page -------------------------
// =========================================================

int Page::put(int address, std::vector<Record> &v) {
    if (address + v.size() > this->sizeInRecords()) {
        fprintf(stderr, "Error: Page put address exceeds page size\n");
        return -1;
    }
    std::copy(v.begin(), v.end(), this->records.begin() + address);
    return 0;
}

// =========================================================
// -------------------------- Storage ----------------------
// =========================================================


void Storage::configure() {
    // TODO: configure MERGE_FAN_IN and MERGE_FAN_OUT

    // calculate the page size in records
    int nBytes = this->BANDWIDTH * this->LATENCY;
    nBytes = ROUNDUP_4K(nBytes);
    this->PAGE_SIZE_IN_RECORDS = std::ceil(nBytes / Config::RECORD_SIZE);

    // calculate the cluster size in pages
    int nRecords = this->CAPACITY_IN_BYTES / Config::RECORD_SIZE;
    int nPages = nRecords / this->PAGE_SIZE_IN_RECORDS;
    this->CLUSTER_SIZE = nPages / (this->MERGE_FAN_IN + this->MERGE_FAN_OUT);

    printv("Configured %s: Page Size: %d records, Cluster Size: %d pages\n",
           this->name.c_str(), this->PAGE_SIZE_IN_RECORDS, this->CLUSTER_SIZE);
}


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

HDD *HDD::instance = nullptr;

int HDD::read(int address, char *buffer, int nBytes) {
    readFile(this->filename, address, buffer, nBytes);
    return nBytes;
}

int HDD::write(int address, char *buffer, int nBytes) {
    writeFile(this->filename, address, buffer, nBytes);
    return nBytes;
}

int HDD::readPage(std::ifstream &file, Page &page) {
    int i = 0;
    while (i < page.sizeInRecords()) {
        file.read(page.records[i].data, Config::RECORD_SIZE);
        if (file.eof()) {
            break;
        }
        i++;
        if (i < page.sizeInRecords()) {
            page.records[i].invalidate();
        }
    }
    return i;
}

int HDD::firstPass(int runSize) {
    std::ifstream file(Config::INPUT_FILE, std::ios::binary);


    int nRecordsReadFromHDD = 0;
    while (nRecordsReadFromHDD < Config::NUM_RECORDS) {
        Page hddpage(this->PAGE_SIZE_IN_RECORDS);
        int nRecords = readPage(file, hddpage);
        nRecordsReadFromHDD += nRecords;
    }
    file.close();
    return runSize;
}

// ---------------------------------------------------------
// --------------------- External Sort ---------------------
// ---------------------------------------------------------

// void externalSort() {

//     std::ifstream input(Config::INPUT_FILE, std::ios::binary);
//     if (!input) {
//         std::cerr << "Error opening input file." << std::endl;
//         exit(1);
//     }

//     SSD *ssd = SSD::getInstance();

//     // Read records into memory and sort cache-size chunks
//     DRAM *dram = DRAM::getInstance();
//     // generate DRAM size runs
//     const int nRecordsInDRAM = Config::DRAM_SIZE / Config::RECORD_SIZE;
//     int nRecordsReadFromHDD = 0;
//     while (nRecordsReadFromHDD < Config::NUM_RECORDS) {
//         // generate a DRAM_size_run
//         int nRecords = dram->generateRun(input, nRecordsInDRAM);
//         nRecordsReadFromHDD += nRecords;
//         flushv();

//         // store the run in SSD
//         int nRecordsStoredInSSD = ssd->storeRun();
//         if (nRecordsStoredInSSD == -1) {
//             fprintf(stderr, "Error: SSD is full\n");
//             exit(1);
//         }
//         if (nRecordsStoredInSSD == 0) {
//             fprintf(stderr, "Error: No records stored in SSD\n");
//             exit(1);
//         }

//         // if (ssd->nRuns == ssd->maxNumRuns) {
//         //     // sort the runs in SSD
//         //     printv("Merge the runs in SSD and store %d records (%d bytes)
//         in
//         //     "
//         //            "HDD\n",
//         //            ssd->nRecords, ssd->nRecords * Config::RECORD_SIZE);
//         //     // TODO: merge the runs in SSD
//         //     ssd->mergeRuns();
//         //     this->nRecords += ssd->nRecords;
//         //     this->nRuns++;
//         //     ssd->reset();
//         //     this->printState();
//         //     goto end;
//         // }
//     }
//     // this->reset();

// end:
//     input.close();
// }


void externalSort() {

    HDD *hdd = HDD::getInstance();
    SSD *ssd = SSD::getInstance();
    DRAM *dram = DRAM::getInstance();

    printv("\nExternal Sort Plan:\n");
    printv("Input size: %sbytes (%s records)\n",
           formatNum(getInputSizeInBytes()).c_str(),
           formatNum(Config::NUM_RECORDS).c_str());


    int SSD_SIZE_RUNS = ssd->MERGE_FAN_IN * ssd->CLUSTER_SIZE;
    printv("\nStep 1: First PASS: generate SSD_SIZE_RUNS: %sBytes\n",
           formatNum(SSD_SIZE_RUNS * Config::RECORD_SIZE).c_str());
    hdd->firstPass(SSD_SIZE_RUNS);

    int pass = 1;
    for (int runSize = SSD_SIZE_RUNS; runSize < Config::NUM_RECORDS;
         runSize *= hdd->MERGE_FAN_IN) {
        printv("\nMERGE PASS %d\n", pass);
        printv("Step: \tMerge %d runs of size %d records from HDD\n",
               hdd->MERGE_FAN_IN, runSize);
        printv("\t\tGenerate %d runs of size %d records in HDD\n",
               hdd->MERGE_FAN_IN, runSize);
        pass++;
    }
}

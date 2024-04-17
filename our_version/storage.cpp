#include "storage.h"
#include <algorithm>
#include <vector>


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

    printv("Configured %4s: Page Size: %4d records (%6sBytes), Cluster Size: %6d "
           "pages\n",
           this->name.c_str(), this->PAGE_SIZE_IN_RECORDS,
           formatNum(this->PAGE_SIZE_IN_RECORDS * Config::RECORD_SIZE).c_str(), this->CLUSTER_SIZE);
}

void Storage::readFrom(const std::string &filePath) {
    if (readFile.is_open())
        readFile.close();
    readFilePath = filePath;
    readFile.open(readFilePath, std::ios::binary);
    assert(readFile.is_open() && "Failed to open read file");
}

void Storage::writeTo(const std::string &filePath) {
    if (writeFile.is_open())
        writeFile.close();
    writeFilePath = filePath;
    writeFile.open(writeFilePath, std::ios::binary);
    assert(writeFile.is_open() && "Failed to open write file");
}

int Storage::readPage(std::ifstream &file, Page *page) { return page->read(file); }

std::vector<Page *> Storage::readNext(int nPages) {
    if (!readFile.is_open()) {
        fprintf(stderr, "Error: Read file is not open\n");
        return {};
    }
    std::vector<Page *> pages;
    for (int i = 0; i < nPages; i++) {
        Page *page = new Page(this->PAGE_SIZE_IN_RECORDS);
        int nRecords = this->readPage(readFile, page);
        if (nRecords == 0) {
            break;
        }
        pages.push_back(page);
    }
    return pages;
}

void Storage::writeNext(std::vector<Page *> pages) {
    if (!writeFile.is_open()) {
        fprintf(stderr, "Error: Write file is not open\n");
        return;
    }
    for (int i = 0; i < pages.size(); i++) {
        pages[i]->write(writeFile);
    }
}


void Storage::closeRead() {
    if (readFile.is_open())
        readFile.close();
}

void Storage::closeWrite() {
    if (writeFile.is_open())
        writeFile.close();
}


// =========================================================
// ------------------------ RunManager ---------------------
// =========================================================


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

void quickSort(Record *records, int nRecords) { quickSortRecursive(records, 0, nRecords - 1); }

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

    printvv("\tRead %d records (%d bytes) From Disk to DRAM and Sorted those\n", nRecords,
            nBytesRead);

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
    // readFile(this->filename, address, buffer, nBytes);
    // printvv("\tRead %d bytes from SSD\n", nBytes);
    return nBytes;
}

int SSD::write(int address, char *buffer, int nBytes) {
    if (address + nBytes > this->maxNumRecords * Config::RECORD_SIZE) {
        fprintf(stderr, "Error: SSD write exceeds SSD capacity\n");
        return -1;
    }
    // writeFile(this->filename, address, buffer, nBytes);
    // printvv("\tWrote %d bytes to SSD\n", nBytes);
    return nBytes;
}

int SSD::storeRun() {
    if (this->nRuns >= this->maxNumRuns) {
        fprintf(stderr, "Error: SSD is storing %d out of %d runs already\n", this->nRuns,
                this->maxNumRuns);
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
    printv("STATE -> Merge the runs in SSD and store %d records in HDD\n", Config::NUM_RECORDS);
    // read the runs from SSD
    int nRuns = this->nRuns;
    int nRecords = this->nRecords;
    Record *records = new Record[nRecords];
    for (int i = 0; i < nRuns; i++) {
        this->read(i * Config::RECORD_SIZE, records[i].data, Config::RECORD_SIZE);
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
        this->write(i * Config::RECORD_SIZE, output[i].data, Config::RECORD_SIZE);
    }
    this->nRecords = nRecords;
    this->nRuns = 1;
    printv("\tMerged %d runs and stored %d records (%d bytes) in SSD\n", nRuns, nRecords,
           nRecords * Config::RECORD_SIZE);
    this->printState();
    return nRecords;
}

void SSD::mergeRunsPlan() {
    printv("STATE -> Merge the runs in SSD and store %d records in HDD\n", Config::NUM_RECORDS);
    // read the runs from SSD to memory
}


// ---------------------------------------------------------
// -------------------------- HDD --------------------------
// ---------------------------------------------------------

HDD *HDD::instance = nullptr;

int HDD::firstPass(int runSize) {

    // std::ifstream file(Config::INPUT_FILE, std::ios::binary);
    // int nRecordsReadFromHDD = 0;
    // while (nRecordsReadFromHDD < Config::NUM_RECORDS) {
    //     Page hddpage(this->PAGE_SIZE_IN_RECORDS);
    //     int nRecords = readPage(file, hddpage);
    //     nRecordsReadFromHDD += nRecords;
    // }
    // file.close();
    // return runSize;
    return 0;
}

void externalSort() {

    HDD *hdd = HDD::getInstance();
    SSD *ssd = SSD::getInstance();
    DRAM *dram = DRAM::getInstance();

    printv("\nExternal Sort Plan:\n");
    printv("Input size: %sbytes (%s records)\n", formatNum(getInputSizeInBytes()).c_str(),
           formatNum(Config::NUM_RECORDS).c_str());
    flushv();


    int SSD_SIZE_RUNS = ssd->MERGE_FAN_IN * ssd->CLUSTER_SIZE;
    printv("\nStep 1: First PASS: generate SSD_SIZE_RUNS: %sBytes\n",
           formatNum(SSD_SIZE_RUNS * Config::RECORD_SIZE).c_str());
    hdd->firstPass(SSD_SIZE_RUNS);

    exit(0); // TODO: remove later

    int pass = 1;
    for (int runSize = SSD_SIZE_RUNS; runSize < Config::NUM_RECORDS; runSize *= hdd->MERGE_FAN_IN) {
        printv("\nMERGE PASS %d\n", pass);
        printv("Step: \tMerge %d runs of size %d records from HDD\n", hdd->MERGE_FAN_IN, runSize);
        printv("\t\tGenerate %d runs of size %d records in HDD\n", hdd->MERGE_FAN_IN, runSize);
        pass++;
    }
}

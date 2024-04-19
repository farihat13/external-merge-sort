#include "Storage.h"

#include <algorithm>
#include <vector>


// =========================================================
// -------------------------- Storage ----------------------
// =========================================================


Storage::Storage(std::string name, ByteCount capacity, int bandwidth, double latency)
    : name(name), CAPACITY_IN_BYTES(capacity), BANDWIDTH(bandwidth), LATENCY(latency) {
    printv("Storage: %6s, Capacity %8sBytes,  Bandwidth %6sBytes/s, "
           "Latency %2.6f seconds\n",
           name.c_str(), formatNum(capacity).c_str(), formatNum(bandwidth).c_str(), latency);
    this->configure();
}


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


RunManager::RunManager(Storage *storage) : storage(storage) {
    baseDir = storage->getName() + "_runs";
    struct stat st = {0};
    if (stat(baseDir.c_str(), &st) == -1) {
        mkdir(baseDir.c_str(), 0700);
    }
    // if (!std::filesystem::exists(baseDir)) {
    //     std::filesystem::create_directory(baseDir);
    // }
}


std::string RunManager::getNextRunFileName() {
    baseDir = storage->getName() + "_runs";
    struct stat st = {0};
    if (stat(baseDir.c_str(), &st) == -1) {
        mkdir(baseDir.c_str(), 0777);
    }
    std::string filename = baseDir + "/r" + std::to_string(nextRunIndex++) + ".bin";
    return filename;
}


// validatations
std::vector<std::string> RunManager::getStoredRunsInfo() {
    std::vector<std::string> runFiles;
    DIR *dir = opendir(baseDir.c_str());
    if (dir) {
        struct dirent *entry;
        while ((entry = readdir(dir)) != nullptr) {
            struct stat path_stat;
            std::string filePath = baseDir + "/" + entry->d_name;
            stat(filePath.c_str(), &path_stat);
            if (S_ISREG(path_stat.st_mode)) {
                runFiles.push_back(entry->d_name);
            }
        }
        closedir(dir);
    }
    std::sort(runFiles.begin(), runFiles.end());
    return runFiles;
}

//
std::vector<std::string> RunManager::getStoredRunsSortedBySize() {
    std::vector<std::string> runFiles = getStoredRunsInfo();
    std::sort(runFiles.begin(), runFiles.end(), [&](const std::string &a, const std::string &b) {
        std::ifstream a_file(baseDir + "/" + a, std::ios::binary | std::ios::ate);
        std::ifstream b_file(baseDir + "/" + b, std::ios::binary | std::ios::ate);
        return a_file.tellg() < b_file.tellg();
    });
    return runFiles;
}

void RunManager::printAllRuns() {
    printv("All runs in %s:\n", baseDir.c_str());
    for (const auto &file : getStoredRunsInfo()) {
        printv("\t%s\n", file.c_str());
    }
}

void RunManager::deleteRun(const std::string &runFilename) {

    std::string fullPath = baseDir + "/" + runFilename;
    if (std::remove(fullPath.c_str()) == 0) {
        printv("Deleted run file: %s\n", fullPath.c_str());
    } else {
        fprintf(stderr, "Error: Cannot delete run file: %s\n", fullPath.c_str());
    }
}

void RunManager::storeRun(std::vector<Page *> &pages) {
    std::string filename = getNextRunFileName();
    RunWriter writer(filename);
    writer.writeNextPages(pages);
}

std::vector<Page *> RunManager::loadRun(const std::string &runFilename, int pageSize) {
    RunReader reader(runFilename, pageSize);
    return reader.readNextPages(pageSize); // Assume this reads the whole file
}

void RunManager::validateRun(const std::string &runFilename, int pageSize) {
    std::vector<Page *> pages = loadRun(runFilename, pageSize);
    for (auto &page : pages) {
        if (!page->isSorted()) {
            throw std::runtime_error("Run is not sorted: " + runFilename);
        }
    }
    // Clean up pages
    for (auto page : pages) {
        delete page;
    }
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

void quickSort(Record *records, int nRecords) { quickSortRecursive(records, 0, nRecords - 1); }

// =========================================================
// -------------------------- Disk -------------------------
// =========================================================


HDD *HDD::instance = nullptr;


HDD::HDD() : Storage("HDD", Config::HDD_SIZE, Config::HDD_BANDWIDTH, Config::HDD_LATENCY) {
    std::ofstream file(this->filename, std::ios::binary);
    file.write("", 1);
    file.close();
    this->runManager = new RunManager(this);
}


HDD::~HDD() {
    if (fileStream.is_open()) {
        fileStream.close();
    }
}

void HDD::write(const char *data, size_t size) { fileStream.write(data, size); }

void HDD::read(char *data, size_t size) { fileStream.read(data, size); }

void HDD::seek(size_t pos) {
    fileStream.seekg(pos, std::ios::beg);
    fileStream.seekp(pos, std::ios::beg);
}


// =========================================================
// -------------------------- DRAM -------------------------
// =========================================================


DRAM *DRAM::instance = nullptr;

DRAM::DRAM() : Storage("DRAM", Config::DRAM_SIZE, Config::DRAM_BANDWIDTH, Config::DRAM_LATENCY) {}
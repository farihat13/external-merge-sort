#include "storage.h"


// =========================================================
// -------------------------- DRAM -------------------------
// =========================================================


DRAM *DRAM::instance = nullptr;

void DRAM::readFrom(int address, char *data, int size) {
    if (address + size > Config::DRAM_SIZE) {
        fprintf(stderr, "DRAM read out of bounds\n");
        exit(1);
    }
    std::memcpy(data, this->data + address, size);
}

void DRAM::writeTo(int address, char *data, int size) {
    if (address + size > Config::DRAM_SIZE) {
        fprintf(stderr, "DRAM write out of bounds\n");
        exit(1);
    }
    std::memcpy(this->data + address, data, size);
}
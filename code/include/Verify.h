#ifndef _VERIFY_H_
#define _VERIFY_H_

#include "Record.h"
#include "config.h"
#include "defs.h"
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <vector>

bool verifyOrder(const std::string &outputFilePath, uint64_t capacityMB);

bool verifyIntegrity(const std::string &inputFilePath, const std::string &outputFilePath,
                     uint64_t capacityMB);

#endif // _VERIFY_H_
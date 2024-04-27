#ifndef _VERIFY_H_
#define _VERIFY_H_

#include "Record.h"
#include "config.h"
#include "defs.h"
#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>

bool verifyOrder(const std::string &outputFilePath, RowCount nRecordsPerRead);

bool verifyIntegrity(const std::string &inputFilePath, const std::string &outputFilePath);

#endif // _VERIFY_H_
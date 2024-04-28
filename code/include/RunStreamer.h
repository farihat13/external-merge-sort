#ifndef _RUNSTREAMER_H_
#define _RUNSTREAMER_H_

#include "Record.h"
#include "Storage.h"
#include "config.h"
#include "defs.h"
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <iostream>
#include <queue>
#include <sys/stat.h>
#include <vector>


// =========================================================
// ----------------------- RunStreamer ---------------------
// =========================================================


enum class StreamerType { INMEMORY_RUN, READER, STREAMER };

/**
 * @brief RunStreamer is a class to stream records from a run
 * It can be used to stream records from a run in memory or from a file
 * If the run is in memory, it will stream records from the run
 * If the run is on disk, it will stream records from the file
 */
class RunStreamer {
  private:
    /** NOTE: must update currentRecord in moveNext */
    Record *currentRecord;
    Record *nextRecord; // currentRecord might be deleted, so keep a copy of nextRecord

    // ===== internal state =====
    // ---- common ----
    StreamerType type;
    Storage *fromDevice = nullptr;
    Storage *toDevice = nullptr;
    // ---- for run ----
    Run *run = nullptr;
    Record *moveNextForRun();
    // ---- for reader ----
    RowCount readSoFar = 0;
    Record *moveNextForReader();
    // ---- for reader and streamer ----
    RunReader *reader = nullptr;
    PageCount readAhead;
    bool inputCluster = false;
    RowCount readAheadPages(PageCount nPages);
    // ---- for streamer ----
    RunStreamer *readStreamer = nullptr;
    std::string writerFilename = "";
    RowCount readStream(RowCount nRecords, bool firstTime = false);
    Record *moveNextForStreamer();

  public:
    // ---- for run ----
    RunStreamer(StreamerType type, Run *run);
    // ---- for reader ----
    RunStreamer(StreamerType type, RunReader *reader, Storage *fromDevice, Storage *toDevice,
                PageCount readAhead, bool inputCluster = false);
    // ---- for streamer ----
    RunStreamer(StreamerType type, RunStreamer *streamer, Storage *fromDevice, Storage *toDevice,
                PageCount readAhead);
    ~RunStreamer();

    // getters
    Record *getCurrRecord() { return currentRecord; }
    RowCount getReadAheadInRecords() {
        if (type == StreamerType::INMEMORY_RUN) { return -1; }
        return readAhead * fromDevice->getPageSizeInRecords();
    }
    Record *moveNext();


    std::string repr() {
        std::string name = "RS:";
        if (reader != nullptr) {
            name += " Reader: " + reader->getFilename();
        } else if (readStreamer != nullptr) {
            name += " Streamer: " + readStreamer->repr();
        } else {
            name += " InMemory";
        }
        return name;
    }

    std::string getFilename() {
        if (type == StreamerType::INMEMORY_RUN) {
            return "InMemory";
        } else if (type == StreamerType::READER) {
            return reader->getFilename();
        } else if (type == StreamerType::STREAMER) {
            return readStreamer->getFilename();
        }
        return "InMemory";
    }

    // default comparison based on first 8 bytes of data
    bool operator<(const RunStreamer &other) const { return *currentRecord < *other.currentRecord; }
    bool operator>(const RunStreamer &other) const { return *currentRecord > *other.currentRecord; }
    // equality comparison based on all bytes of data
    bool operator==(const RunStreamer &other) const {
        return *currentRecord == *other.currentRecord;
    }

    char *reprKey() { return currentRecord->reprKey(); }
}; // class RunStreamer


#endif // _RUNSTREAMER_H_
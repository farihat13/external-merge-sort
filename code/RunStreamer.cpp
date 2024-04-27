
#include "RunStreamer.h"
#include "Losertree.h"
#include "Storage.h"
#include <algorithm>
#include <cmath>
#include <vector>

// =========================================================
// ----------------------- RunStreamer ---------------------
// =========================================================

RunStreamer::~RunStreamer() {
    if (run != nullptr) {
        delete run;
        run = nullptr;
    }
    if (reader != nullptr) {
        delete reader;
        reader = nullptr;
    }
    if (readStreamer != nullptr) {
        delete readStreamer;
        readStreamer = nullptr;
    }
    printv("\t\t\t\tRunStreamer %s destroyed\n", getName().c_str());
}


// -------------------- Stream from Run --------------------

/**
 * Runstreamer for a run
 */
RunStreamer::RunStreamer(StreamerType type, Run *run) : type(type), run(run) {
    assert(type == StreamerType::INMEMORY_RUN); // validate
    /**
     * set the current record to the head of the run
     */
    currentRecord = run->getHead();
    if (currentRecord == nullptr) { // validate
        throw std::runtime_error("ERROR: RunStreamer initialized with empty run");
    }
}

Record *RunStreamer::moveNextForRun() {
    if (currentRecord->next == nullptr) {
        /** reached the end of the run */
        currentRecord = nullptr;
        // if (run != nullptr) { // free memory
        //     delete run;
        //     run = nullptr;
        // }
        return nullptr;
    }
    /**
     * move to the next record in the run
     */
    currentRecord = currentRecord->next;
    return currentRecord;
}


// -------------------- Stream from RunReader --------------------


/**
 * Runstreamer for a runreader
 */
RunStreamer::RunStreamer(StreamerType type, RunReader *reader, Storage *fromDevice,
                         Storage *toDevice, PageCount readAhead)
    : type(type), reader(reader), fromDevice(fromDevice), toDevice(toDevice), readAhead(readAhead) {

    TRACE(true);

    assert(type == StreamerType::READER); // validate
    if (readAhead < 1) {                  // validate
        throw std::runtime_error("Error: ReadAhead should be at least 1");
    }

    /**
     * 1. readAhead from the reader, this should create a run and store in memory `run` variable
     */
    RowCount nRecords = readAheadPages(readAhead);
    if (nRecords == 0 || run == nullptr) { // validate
        throw std::runtime_error("ERROR: RunStreamer initialized with empty run");
    }

    /**
     * 2. set the current record to the head of the run
     * */
    currentRecord = run->getHead();
    if (currentRecord == nullptr) { // validate
        std::string errorMsg =
            "ERROR: readAhead" + std::to_string(nRecords) + "  records but failed to read run";
        throw std::runtime_error(errorMsg);
    }
    // debug
    printv("\t\t\tRunStreamer %s initialized, curr_rec %s\n", getName().c_str(),
           currentRecord->reprKey());
    flushv();
}


RowCount RunStreamer::readAheadPages(PageCount nPages) {
    // TRACE(true);
    /**
     * 1. read `nPages` pages from the reader
     */
    RowCount pageSize = fromDevice->getPageSizeInRecords();
    RowCount nRecordsToRead = nPages * pageSize;
    RowCount nRecords = nRecordsToRead;
    Record *runHead = reader->readNextRecords(&nRecords);
    RowCount nRecordsRead = nRecords;
    if (nRecordsRead < nRecordsToRead) {
        /**
         * 1.1 if less than `nRecordsToRead` records are read, that means the reader has reached the
         * end of the file. So, close the reader and delete the file
         */
        if (!reader->isDeletedFile()) {
            fromDevice->freeSpace(reader->getFilesize());
            reader->close();
            reader->deleteFile();
        }
    }
    // if (run != nullptr) { // free memory
    //     delete run;
    //     run = nullptr;
    // }
    /**
     * 2. create a run from the records read, and store in memory `run` variable
     */
    run = new Run(runHead, nRecordsRead);
    return nRecordsRead;
}


Record *RunStreamer::moveNextForReader() {
    if (currentRecord == nullptr) {
        /** no more records in the reader */
        return nullptr;
    }
    if (currentRecord->next == nullptr) {
        /**
         * if this is the last record in the run,
         * 1. read next `readAhead` pages, which will create a new run and store in memory
         *      1.1. if no records are read, set the current record to null and return nullptr
         */
        RowCount nRecords = readAheadPages(readAhead);
        if (nRecords == 0) {
            currentRecord = nullptr;
            return nullptr;
        }
        /**
         * 2. set the current record to the head of the run
         */
        currentRecord = run->getHead();
    } else {
        /**
         * if this is not the last record in the run, move to the next record
         */
        currentRecord = currentRecord->next;
    }
    return currentRecord;
}


// -------------------- Stream from RunStreamer --------------------


/**
 * Runstreamer for a runstreamer
 */
RunStreamer::RunStreamer(StreamerType type, RunStreamer *streamer, Storage *fromDevice,
                         Storage *toDevice, PageCount readAhead)
    : type(type), readStreamer(streamer), fromDevice(fromDevice), toDevice(toDevice),
      readAhead(readAhead) {
    TRACE(true);
    assert(type == StreamerType::STREAMER); // validate
    if (readAhead < 1) {                    // validate
        throw std::runtime_error("Error: ReadAhead should be at least 1");
    }

    /**
     * 1. intialize the inputbuffer filename where the streamer data is stored
     */
    std::string fname = streamer->getFilename();
    fname = fname.substr(fname.find_last_of("/") + 1);
    writerFilename = fromDevice->getBaseDir() + "/buf_" + fname;
    /**
     * 2. store `nInBufRecords` Records in a buffer file in `fromDevice`
     */
    RowCount nInBufRecords = streamer->getReadAheadInRecords();
    RowCount nRecordsBuffered = readStream(nInBufRecords);
    if (nRecordsBuffered == 0) { // validate
        throw std::runtime_error("ERROR: RunStreamer initialized with empty run");
    }
    /**
     * 3. create a reader for the file and read `readAhead` pages from that file to create a run
     *      the run is stored in memory in `run`
     */
    reader = new RunReader(writerFilename, nRecordsBuffered, fromDevice->getPageSizeInRecords());
    RowCount nRecordsReadAhead = readAheadPages(readAhead);
    if (nRecordsReadAhead == 0) { // validate
        std::string errorMsg = "ERROR: Buffered" + std::to_string(nRecordsBuffered) +
                               "  records but failed to readAhead";
        throw std::runtime_error(errorMsg);
    }
    /**
     * 4. set the current record to the head of the run
     */
    currentRecord = run->getHead();
    if (currentRecord == nullptr) {
        std::string errorMsg = "ERROR: readAhead" + std::to_string(nRecordsReadAhead) +
                               "  records but failed to read run";
        throw std::runtime_error(errorMsg);
    }
    printv("\t\t\tRunStreamer initialized with STREAMER %s, curr_rec: %s\n",
           streamer->getName().c_str(), currentRecord->reprKey());
    flushv();
}


RowCount RunStreamer::readStream(RowCount nRecords) {
    /**
     * 1. read nRecords from the streamer and create a run (linked list of records)
     */
    Record *head = new Record();
    Record *curr = head;
    RowCount count = 0;
    while (count < nRecords) {
        Record *rec = readStreamer->getCurrRecord();
        if (rec == nullptr) { break; }
        if (count == 0) printv("\t\t\tFirst record: %s\n", rec->reprKey());
        if (count == 1) printv("\t\t\tSecond record: %s\n", rec->reprKey());
        count++;
        curr->next = rec;
        curr = rec;
        if (count < nRecords - 1) { Record *r = readStreamer->moveNext(); }
    }
    printv("\t\t\t\tRead %lld records\n", count);
    printv("\t\t\t\tCreated runwriter %s from streamer filename %s\n", writerFilename.c_str(),
           readStreamer->getName().c_str());
    flushv();
    Run run = Run(head->next, count);

    /**
     * 2. write the run to a file in `fromDevice`
     */
    RunWriter writer(writerFilename); // this opens the file in truncate mode
    writer.writeNextRun(run);
    writer.close();
    // // free memory
    // delete head;
    // // delete run; // the ~Run() will delete the records
    /**
     * 3. update the input cluster space of the `fromDevice`
     */
    fromDevice->fillInputCluster(count);
    // print access time
    printv("\t\t\t\tSTATE -> Wrote %lld records to %s using RS\n", count, writerFilename.c_str());
    printv("\t\t\t\tACCESS -> A write to %s was made with size %llu bytes and latency %d ms\n",
           fromDevice->getName().c_str(), count * Config::RECORD_SIZE,
           fromDevice->getAccessTimeInMillis(count));
    flushv();
    // return the number of records written
    return count;
}

Record *RunStreamer::moveNextForStreamer() {
    moveNextForReader();
    if (currentRecord == nullptr) {
        /**
         * if this is the last record in the run,
         * 1. read stream and update the buffer file
         */
        RowCount nInBufRecords = readStreamer->getReadAheadInRecords();
        printv("\t\t\t\tBuffered %lld records from %s has been exhausted\n", nInBufRecords,
               readStreamer->getName().c_str());
        RowCount nRecordsBuffered = readStream(nInBufRecords);
        if (nRecordsBuffered == 0) {
            currentRecord = nullptr;
            return nullptr;
        }
        // // free memory
        // if (run != nullptr) {
        //     delete run;
        //     run = nullptr;
        // }
        // if (reader != nullptr) {
        //     delete reader;
        //     reader = nullptr;
        // }
        /**
         * 2. create a reader for the buffer file and read `readAhead` pages to create a run
         */
        reader =
            new RunReader(writerFilename, nRecordsBuffered, fromDevice->getPageSizeInRecords());
        RowCount nRecordsReadAhead = readAheadPages(readAhead);
        if (nRecordsReadAhead == 0) { // validate
            std::string errorMsg = "ERROR: Buffered" + std::to_string(nRecordsBuffered) +
                                   "  records but failed to readAhead";
            throw std::runtime_error(errorMsg);
        }
        /**
         * 3. set the current record to the head of the run
         */
        currentRecord = run->getHead();
        if (currentRecord == nullptr) {
            std::string errorMsg = "ERROR: readAhead" + std::to_string(nRecordsReadAhead) +
                                   "  records but failed to read run";
            throw std::runtime_error(errorMsg);
        }
    }

    // printv("moving next to %s for streamer %s\n", currentRecord->reprKey(), getName().c_str());
    return currentRecord;
}


// -------------------- Move Next --------------------


Record *RunStreamer::moveNext() {
    if (type == StreamerType::INMEMORY_RUN) {
        return moveNextForRun();
    } else if (type == StreamerType::READER) {
        return moveNextForReader();
    } else if (type == StreamerType::STREAMER) {
        return moveNextForStreamer();
    } else {
        throw std::runtime_error("Error: Invalid StreamerType");
    }
}

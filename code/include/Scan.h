#ifndef _SCAN_H_
#define _SCAN_H_

#include "Iterator.h"
#include <fstream>


class ScanPlan : public Plan {
    friend class ScanIterator;

  public:
    ScanPlan(RowCount const count, std::string const filename);
    ~ScanPlan();
    Iterator *init() const;

  private:
    RowCount const _count;
    std::string const _filename;
}; // class ScanPlan


class ScanIterator : public Iterator {
  public:
    ScanIterator(ScanPlan const *const plan);
    ~ScanIterator();
    bool next();
    void getRecord(Record *r);
    void getPage(Page *p);

  private:
    ScanPlan const *const _plan;
    RowCount _count;

    // HDD *_hdd;
    std::ifstream _file;
    // Page *_currpage;
}; // class ScanIterator

#endif // _SCAN_H_
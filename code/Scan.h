#ifndef SCAN_H
#define SCAN_H

#include "Iterator.h"
#include <fstream>


class ScanPlan : public Plan {
    friend class ScanIterator;

  public:
    ScanPlan(RowCount const count);
    ~ScanPlan();
    Iterator *init() const;

  private:
    RowCount const _count;
}; // class ScanPlan


class ScanIterator : public Iterator {
  public:
    ScanIterator(ScanPlan const *const plan);
    ~ScanIterator();
    bool next();
    void getRecord(char *s);

  private:
    ScanPlan const *const _plan;
    RowCount _count;

    std::ifstream file;

    void gen_a_record(char *s, const int len);
}; // class ScanIterator

#endif // SCAN_H
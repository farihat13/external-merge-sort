#ifndef ITERATOR_H
#define ITERATOR_H

#include "defs.h"
#include "Record.h"

typedef uint64_t RowCount;

class Plan {
    friend class Iterator;

  public:
    Plan();
    virtual ~Plan();
    virtual class Iterator *init() const = 0;

  private:
}; // class Plan

class Iterator {
  public:
    Iterator();
    virtual ~Iterator();
    void run();
    virtual bool next() = 0;
    virtual void getRecord(Record *r) = 0;

  private:
    RowCount _count;
}; // class Iterator

#endif // ITERATOR_H
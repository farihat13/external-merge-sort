#ifndef _ITERATOR_H_
#define _ITERATOR_H_

#include "Record.h"
#include "Storage.h"
#include "config.h"
#include "defs.h"


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
    virtual void getPage(Page *p) = 0;
    virtual void get(char *data, ByteCount nBytes);

  private:
    RowCount _count;
}; // class Iterator

#endif // _ITERATOR_H_
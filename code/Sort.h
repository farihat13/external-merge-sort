#ifndef _SORT_H_
#define _SORT_H_


#include "Iterator.h"


class SortPlan : public Plan {
    friend class SortIterator;

  public:
    SortPlan(Plan *const input);
    ~SortPlan();
    Iterator *init() const;

  private:
    Plan *const _input;
}; // class SortPlan


class SortIterator : public Iterator {
  public:
    SortIterator(SortPlan const *const plan);
    ~SortIterator();
    bool next();
    void getRecord(Record *r);

  private:
    SortPlan const *const _plan;
    Iterator *const _input;
    RowCount _consumed, _produced;
}; // class SortIterator


#endif // _SORT_H_
#ifndef FILTER_H
#define FILTER_H

#include "Iterator.h"


class FilterPlan : public Plan {
    friend class FilterIterator;

  public:
    FilterPlan(Plan *const input);
    ~FilterPlan();
    Iterator *init() const;

  private:
    Plan *const _input;
}; // class FilterPlan


class FilterIterator : public Iterator {
  public:
    FilterIterator(FilterPlan const *const plan);
    ~FilterIterator();
    bool next();
    void getRecord(Record *r);

  private:
    FilterPlan const *const _plan;
    Iterator *const _input;
    RowCount _consumed, _produced;
}; // class FilterIterator

#endif // FILTER_H
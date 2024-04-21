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
    void getPage(Page *p);

  private:
    SortPlan const *const _plan;
    Iterator *const _input;
    RowCount _consumed, _produced;

    // ==== external merge sort ====

    void externalMergeSort();

    // utility variables for external merge sort
    HDD *_hdd;
    SSD *_ssd;
    DRAM *_dram;
    RowCount _hddCapacity;
    RowCount _hddPageSize;
    RowCount _ssdCapacity;
    RowCount _ssdPageSize;
    RowCount _dramCapacity;
    RowCount _dramPageSize;

    // utility functions for external merge sort
    /**
     * @brief Load input data from Disk to DRAM
     * @param pages: pointer to the first page in DRAM
     * @return number of records read
     */
    RowCount loadInputToDRAM(Page **pages);
    void firstPass();
}; // class SortIterator


#endif // _SORT_H_
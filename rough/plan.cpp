#include <iostream>
#include <vector>
#include <unordered_set>

class Plan {
  public:
    virtual ~Plan() {}
};

class Iterator {
  public:
    virtual ~Iterator() {}
    virtual bool next() = 0;
};

class FilterPlan : public Plan {
  public:
    FilterPlan(std::vector<int> const &input) {
        for (const auto &i : input) {
            _input.insert(i);
        }
    }
    ~FilterPlan() {}

    Iterator *init() const;

  private:
    std::unordered_set<int> _input;
    friend class FilterIterator;
};

class FilterIterator : public Iterator {
  public:
    FilterIterator(FilterPlan const *const plan)
        : _plan(plan), _it(_plan->_input.begin()) {}
    ~FilterIterator() {}

    bool next() {
        if (_it != _plan->_input.end()) {
            ++_it;
            return _it != _plan->_input.end();
        }
        return false;
    }

  private:
    FilterPlan const *const _plan;
    std::unordered_set<int>::const_iterator _it;
};

Iterator *FilterPlan::init() const {
    return new FilterIterator(this);
}

int main() {
    std::vector<int> data = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4};
    FilterPlan plan(data);
    Iterator *it = plan.init();
    while (it->next()) {
        std::cout << "Found a unique number.\n";
    }
    delete it;
    // RowCount n = 100;
    // Plan * const plan = new SortPlan( new ScanPlan(n));

    // Iterator * const it = plan->init ();
    // it->run ();
    // delete it;

    // delete plan;
    return 0;
}
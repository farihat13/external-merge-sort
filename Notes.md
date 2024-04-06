# Project: External Merge Sort

## Starter Code

### `defs.c`, `defs.h`, `Assert.cpp`

```c
void Assert (bool const predicate, char const * const file, int const line);
#if defined ( _DEBUG )  ||  defined (DEBUG)
#define DebugAssert(b) Assert ((b), __FILE__, __LINE__)
...
```

This Assert function is a common way to catch bugs in programming. The idea is to call Assert with a condition that you expect to be true at a certain point in your program. If the condition is not true, then there's a bug somewhere. The file name and line number are useful for locating where the failed assertion happened.
`__FILE__` and `__LINE__` are predefined macros in C and C++.
`__FILE__` is a string that contains the name of the current source file with any directory components removed.
`__LINE__`is an integer that is the line number at the location in the source file where `__LINE__` is used.

**Summary:**
use `DebugAssert`, `FinalAssert`, `ParamAssert`, `traceprintf` for debugging

```c
#define TRACE(trace) Trace __trace (trace, __FUNCTION__, __FILE__, __LINE__)
```

When you use `TRACE(trace)` in your code, it will be replaced by a creation of a Trace object named `__trace`. This macro is likely used to create a Trace object that records where it was created for debugging or logging purposes.
I can use this to kind of isolate a particular functions print statements. since `Trace(true)` will print `>>>>` and at the end of
function destructor will print `<<<<<`.

```c
void calculateSomething() {
    TRACE(true); 
    // Your code here...
    // The Trace object will automatically stop tracing when it goes out of scope
}
```

These template helper functions work with integer, double etc diverse data types.

```
template <class Val> inline Val divide (Val const a, Val const b)
```

### `Iterator.h`, `Iterator.cpp`

The `Plan` class is a base class for creating different types of plans.
The `Plan` class also declares `Iterator` as a friend class, which means Iterator has access to the private and protected members of Plan.
The `init()` function is declared as `= 0`, which means it's a pure virtual function and must be overridden by any non-abstract derived class.
The Iterator class is a base class for creating different types of iterators.
When you're working with inheritance and pointers to base class, a virtual destructor is necessary to ensure proper cleanup of derived class objects. When a derived class object is deleted through a pointer-to-base, and the base class destructor is not virtual, the derived class destructor is not invoked leading to incomplete cleanup.
With virtual, the call to the destructor is resolved at runtime (this is known as dynamic binding), so the correct destructor is called.

### `Filter.h`, `Filter.c`

`Iterator * init () const;` The `const` means that this is a const member function. A const member function promises not to modify any member variables of the class (unless they are declared as `mutable`). It also doesn't allow calling other non-const member functions of the class. This is useful when you want to provide a function that can be called on a const object of the class, or when you want to make it clear that the function doesn't modify the object.

The `FilterPlan` and `FilterIterator` classes seem to be part of a larger system for processing or iterating over some kind of data. The `FilterPlan` class likely represents a plan for filtering data, and the `FilterIterator` class likely represents an iterator that can iterate over a `FilterPlan` and perform the filtering. The exact details would depend on the implementation of the methods and the overall design of the system.

Dummy usage:

```c
#include <iostream>
#include <vector>

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
    FilterPlan(std::vector<int> const& input) : _input(input) {}
    ~FilterPlan() {}

    Iterator* init() const;

private:
    std::vector<int> const _input;
};

class FilterIterator : public Iterator {
public:
    FilterIterator(FilterPlan const* const plan, int index = 0)
        : _plan(plan), _index(index) {}
    ~FilterIterator() {}

    bool next() {
        while (_index < _plan->_input.size()) {
            if (_plan->_input[_index++] % 2 != 0) {
                return true;
            }
        }
        return false;
    }

private:
    FilterPlan const* const _plan;
    int _index;
};

Iterator* FilterPlan::init() const {
    return new FilterIterator(this);
}

int main() {
    std::vector<int> data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    FilterPlan plan(data);
    Iterator* it = plan.init();
    while (it->next()) {
        std::cout << "Found an odd number.\n";
    }
    delete it;
    return 0;
}
```

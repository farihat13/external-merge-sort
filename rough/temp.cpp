#include <iostream>

struct MyStruct {
    int x;
    char c[];
};

class MyClass {
    int x;
    char c[];
public:
    MyClass(int val) : x(val) {}
};

int main() {
    std::cout << "Size of MyStruct: " << sizeof(MyStruct) << " bytes\n";
    std::cout << "Size of MyClass: " << sizeof(MyClass) << " bytes\n";
    return 0;
}

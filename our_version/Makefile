# Compiler settings
CPPOPT=-g -Og -D_DEBUG -D_SMALLCONFIG -D_USE_LOGFILE -D_SEED
CXX = g++
CXXFLAGS =$(CPPOPT) -Wall -std=c++11

# Object files
OBJS = common.o storage.o externalsort.o

# Executable name
TARGET = externalsort

# Build rules
$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJS)

common.o: common.cpp common.h
	$(CXX) $(CXXFLAGS) -c common.cpp

storage.o: storage.cpp storage.h common.h
	$(CXX) $(CXXFLAGS) -c storage.cpp

externalsort.o: externalsort.cpp storage.h common.h
	$(CXX) $(CXXFLAGS) -c externalsort.cpp

# run rule
run: $(TARGET)
	./$(TARGET) -c 100 -s 200 -o trace

# Clean rule
clean:
	rm -f $(OBJS) $(TARGET)

cleanall:
	rm -f $(OBJS) $(TARGET) input.txt log.txt readable_input.txt trace.txt cache.tmp
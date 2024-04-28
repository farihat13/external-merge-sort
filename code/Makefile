# compiler options
CPPOPT=-g -Og -D_DEBUG -D_VALIDATE -D_VERIFY #-D_SMALL #-D_USE_LOGFILE #-D_DUP_CHECK #-DUP_GEN
# -O2 -Os -Ofast
# -fprofile-generate -fprofile-use
# -fsanitize=address -fsanitize=leak -fsanitize=undefined

# compiler flags
CPPFLAGS=$(CPPOPT) -Wall -ansi -pedantic -std=c++11 -Iinclude
# -Wparentheses -Wno-unused-parameter -Wformat-security
# -fno-rtti -std=c++11 -std=c++98

# headers and code sources
HDRS=$(wildcard include/*.h)
SRCS=$(wildcard *.cpp)
TARGET=ExternalSort.exe

# compilation targets
OBJS=$(SRCS:%.cpp=%.o)

# pattern rules for object files
%.o: %.cpp $(HDRS)
	g++ $(CPPFLAGS) -c $< -o $@

# default target
$(TARGET) : $(OBJS)
	g++ $(CPPFLAGS) -o ExternalSort.exe $(OBJS)

trace : ExternalSort.exe Makefile
	@date > trace
	./ExternalSort.exe >> trace
	@size -t ExternalSort.exe $(OBJS) | sort -r >> trace

# run rule
run: $(TARGET)
	./$(TARGET) -c 2500000 -s 1000

run1G: $(TARGET)
	./$(TARGET) -c 1200000 -s 1000

run20G: $(TARGET)
	./$(TARGET) -c 20971520 -s 1024

run125G: $(TARGET)
	./$(TARGET) -c 131072000 -s 1024

run2G: $(TARGET)
	@date > trace
	./$(TARGET) -c 3000000 -s 1000 >> trace
	@size -t $(TARGET) $(OBJS) | sort -r >> trace

run100M: $(TARGET)
#	@date > trace
	./$(TARGET) -c 100000 -s 1000 
#	@size -t $(TARGET) $(OBJS) | sort -r >> trace

list : Makefile
	echo Makefile $(HDRS) $(SRCS) $(DOCS) $(SCRS) > list
count : list
	@wc `cat list`

clean :
	@rm -f $(OBJS) ExternalSort.exe ExternalSort.exe.stackdump trace

cleanall:
	@rm -f $(OBJS) ExternalSort.exe ExternalSort.exe.stackdump trace list count input*.txt output*.txt
	@rm -rf HDD_runs SSD_runs DRAM_runs *.tmp Verify_parts
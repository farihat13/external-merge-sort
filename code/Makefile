CPPOPT=-g -Og #-D_DEBUG
# -O2 -Os -Ofast
# -fprofile-generate -fprofile-use
CPPFLAGS=$(CPPOPT) -Wall -ansi -pedantic -std=c++11 \
-Wparentheses -Wno-unused-parameter -Wformat-security
# -fno-rtti -std=c++11 -std=c++98

# documents and scripts
DOCS=Tasks.txt
SCRS=

# headers and code sources
HDRS=	defs.h \
		Iterator.h Scan.h Filter.h Sort.h
SRCS=	defs.cpp Assert.cpp ExternalSort.cpp \
		Iterator.cpp Scan.cpp Filter.cpp Sort.cpp

# compilation targets
OBJS=	defs.o Assert.o ExternalSort.o \
		Iterator.o Scan.o Filter.o Sort.o

# RCS assists
REV=-q -f
MSG=no message

# default target
#
ExternalSort.exe : Makefile $(OBJS)
	g++ $(CPPFLAGS) -o ExternalSort.exe $(OBJS)

trace : ExternalSort.exe Makefile
	@date > trace
	./ExternalSort.exe >> trace
	@size -t ExternalSort.exe $(OBJS) | sort -r >> trace

$(OBJS) : Makefile defs.h
ExternalSort.o : Iterator.h Scan.h Filter.h Sort.h
Iterator.o Scan.o Filter.o Sort.o : Iterator.h
Scan.o : Scan.h
Filter.o : Filter.h
Sort.o : Sort.h

list : Makefile
	echo Makefile $(HDRS) $(SRCS) $(DOCS) $(SCRS) > list
count : list
	@wc `cat list`

ci :
	ci $(REV) -m"$(MSG)" $(HDRS) $(SRCS) $(DOCS) $(SCRS)
	ci -l $(REV) -m"$(MSG)" Makefile
co :
	co $(REV) -l $(HDRS) $(SRCS) $(DOCS) $(SCRS)

clean :
	@rm -f $(OBJS) ExternalSort.exe ExternalSort.exe.stackdump trace

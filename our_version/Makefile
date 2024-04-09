SRCS=$(wildcard *.cpp)

CPPFLAGS= -Wall -ansi -pedantic -std=c++11 

OBJS=$(SRCS:.cpp=.o)

TARGS=$(SRCS:.cpp=)

all: $(TARGS)

# $(TARGS): $(OBJS)

# and finally, a clean line
clean: 
	rm -f $(OBJS) $(TARGS) *.out


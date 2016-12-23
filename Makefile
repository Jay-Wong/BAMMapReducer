#MapReducer makefile

TARGET=./bin/MapReducer
#CXX := purify -follow-child-processes -max_threads=1000 g++

DEPLIBS += -lpthread

MapReducer_SRCS = $(shell ls src/*.cpp)
MapReducer_OBJS = $(subst src/,tmp/,$(subst .cpp,.o,$(MapReducer_SRCS)))

ALL_OBJS = ${MapReducer_OBJS}

all:    $(TARGET) CHATRProc

$(TARGET): $(ALL_OBJS) 
	$(CXX) $(LIBS) $(ALL_OBJS) $(DEPLIBS) $(LDFLAGS) -g -o $(TARGET)

$(MapReducer_OBJS): tmp/%.o:	src/%.cpp
	$(CXX) $(CXXFLAGS) -c $< -g -o $@

CHATRProc:$(TARGET)

clean:
	$(RM) tmp/*.o $(TARGET)

depend:
	makedepend -m $(CXXFLAGS) $(SRCS)  

ctags:
	ctags `find . -name "*.h"` `find . -name "*.cpp"` 

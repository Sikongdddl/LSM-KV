
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness: skiplist.o SSTable.o bloomFliter.o kvstore.o correctness.o

persistence: skiplist.o SSTable.o bloomFliter.o kvstore.o persistence.o

clean:
	-rm -f correctness persistence *.o

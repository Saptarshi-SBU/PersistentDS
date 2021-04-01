TARGET = btrdb
CC = g++
CFLAGS = -std=c++11 -g -DBOOST_LOG_DYN_LINK -DLOGLEVELINFO
LIBINC = -lboost_log -lpthread -lboost_thread -lboost_iostreams -lboost_filesystem -lprotobuf

PROTOC = protoc
PROTOFILE = meta.proto
PFLAGS = -I=. --cpp_out=.

SRC =btnode.cpp
SRC+=btree.cpp
SRC+=bptree.cpp
SRC+=boost_logger.cpp
SRC+=meta.pb.cc
SRC+=main.cpp

all: 
	$(PROTOC) $(PFLAGS) $(PROTOFILE)
	$(CC) $(CFLAGS) $(LIBINC) -o $(TARGET) $(SRC)
clean:
	rm -f $(TARGET)

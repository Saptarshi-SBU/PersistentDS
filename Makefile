TARGET = btrdb
CC = g++
CFLAGS = -std=c++11 -g -DBOOST_LOG_DYN_LINK
LIBINC = -lboost_log -lpthread -lboost_system -lboost_date_time -lboost_thread

#CFLAGS = -std=c++11 -g -DDEBUG

SRC =btnode.cpp
SRC+=btree.cpp
SRC+=bptree.cpp
SRC+=boost_logger.cpp
SRC+=main.cpp

all: 
	$(CC) $(CFLAGS) $(LIBINC) -o $(TARGET) $(SRC)
clean:
	rm -f $(TARGET)

TARGET = btree
CC = g++
CFLAGS = -std=c++11 -g -DDEBUG

SRC =btnode.cpp
SRC+=btree.cpp
SRC+=bptree.cpp
SRC+=main.cpp

all: 
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC)
clean:
	rm -f $(TARGET)

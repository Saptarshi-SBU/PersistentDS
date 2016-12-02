/*-------------------------------------------------------
 *
 *  g++ -std=c++11 -o main main.cpp btnode.cpp btree.cpp
 *
 * ------------------------------------------------------*/
#include <iostream>
#include <chrono>

#include "btnode.h"
#include "btree.h"
#include "bptnode.h"

#define _TEST_NODE_ 0
#define _TEST_TREE_ 1
#define _TEST_DELETE_ 1
#define _FORWARD_DELETE 1
#define _REVERSE_DELETE 0

int main(int argc, char **argv) {

	if (argc < 3) {
		std::cout << "Usage :" << "program <degree> <node_count>" << endl;
		return -1;
	}

	int fanout = atoi(argv[1]);
	if (fanout < 3) {
		cout << "invalid argument : min b-tree fanout is 3 " << endl;
		return -1;
	}

	int nodes  = atoi(argv[2]);
	
#if _TEST_NODE_

	shared_ptr<btnode> node (new btnode());
	for (int i = 0;i < 3; i++)
		node->_insert_key(100 + i, value_t(NULL, 1000));
	node->_insert_key(99, value_t(NULL, 1000));
	node->_print();
	int pos = node->_find_key(102);
	cout << "Pos " << pos << endl;
	auto v = node->_keysAt(pos);
	cout << "Value " << v.second._off << endl;
	cout << "Keys Size " << node->_num_keys() << endl;
	for (int i = 0;i < 100; i++)
		node->_remove_key(100 + i);
	node->_print();
#endif

#if _TEST_TREE_
	btree* bt = new btree(fanout);

	auto t1 = std::chrono::high_resolution_clock::now();
	for (int i = 1;i <= nodes; i++) {
		bt->_insert(i, value_t(NULL, 1000));
	}
	auto t2 = std::chrono::high_resolution_clock::now();
	bt->_print();
	std::chrono::duration<double, std::milli> ins_fp_ms = t2 - t1;
#if _TEST_DELETE_

	auto t3 = std::chrono::high_resolution_clock::now();
#if _FORWARD_DELETE
	for (int i = 1;i <= nodes; i++) {
#elif _REVERSE_DELETE
	for (int i = nodes;i >= 0; i--) {
#endif
		bt->_delete(i);
	}        
	auto t4 = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::milli> del_fp_ms = t4 - t3;
#endif
	bt->_print();
 	delete bt;
#endif
	cout << "################## Printing B-Tree Stats #####################" << endl;
	cout << " insert : " << ins_fp_ms.count() << " msecs " << endl; 
	cout << " delete : " << del_fp_ms.count() << " msecs " << endl; 
	return 0;
}

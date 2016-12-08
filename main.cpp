/*-------------------------------------------------------
 *
 *  g++ -std=c++11 -o main main.cpp btnode.cpp btree.cpp
 *
 * ------------------------------------------------------*/
#include <iostream>
#include <chrono>
#include <errno.h>
#include <unistd.h>

#include "btnode.h"
#include "btree.h"

#include "bptnode.h"
#include "bptree.h"

#define _TEST_NODE_ 0
#define _TEST_BPNODE_ 0
#define _TEST_BPINTERNALNODE_ 0
#define _TEST_TREE_ 0
#define _TEST_BPTREE_ 1
#define _TEST_DELETE_ 0
#define _FORWARD_DELETE 0
#define _REVERSE_DELETE 0

#define DEFAULT_BRANCH_FACTOR 3
#define DEFAULT_NUM_KEYS 1

struct options {
	int fanout;
	int nr_keys;
};

int
parse(int argc, char **argv, struct options* opt) {
	int c = 0;

	while ((c = getopt(argc, argv, "b:n:")) != -1) {

        	switch(c) {
       		case 'b':
	    	if (optarg)
                    opt->fanout = atoi(optarg);
                break;

       		case 'n':
	    	if (optarg)
                    opt->nr_keys = atoi(optarg);
                break;
               
                default:
                cerr << "Usage : [-b] fanout [-n] number of keys " << endl;
                return -EINVAL;
               }        
        }

        return 0;
}        

int main(int argc, char **argv) {

	struct options opt = { DEFAULT_BRANCH_FACTOR,DEFAULT_NUM_KEYS };

	if (parse(argc, argv, &opt) < 0)
		return -1;

	if (opt.fanout < 3) {
		cout << "invalid argument : min b-tree fanout is 3 " << endl;
		return -EINVAL;
	}

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

#if _TEST_BPNODE_

	auto node = blkptr_leaf_t (new bptnode_leaf(blkptr_internal_t(nullptr), 0));
	for (int i = 0;i < 3; i++)
		node->insert_record(100 + i, mapping_t(NULL, 1000, 32));
	node->insert_record(99, mapping_t(NULL, 1000, 32));
	node->print();
	int pos = node->find_key(102);
	cout << "Pos " << pos << endl;
	auto v = node->_keysAt(pos);
	cout << "Value " << v << endl;
	cout << "Keys Size " << node->_num_keys() << endl;
	for (int i = 0;i < 100; i++)
		node->remove_key(100 + i);
	node->print();
#endif

#if _TEST_BPINTERNALNODE_

	auto node = blkptr_internal_t (new bptnode_internal(blkptr_internal_t(nullptr), 0));
	for (int i = 0;i < 3; i++)
		node->insert_key(100 + i);//, mapping_t(NULL, 1000, 32));
	node->insert_key(99);//, mapping_t(NULL, 1000, 32));
	node->print();
	int pos = node->find_key(102);
	cout << "Pos " << pos << endl;
	auto v = node->_keysAt(pos);
	cout << "Value " << v << endl;
	cout << "Keys Size " << node->_num_keys() << endl;
	for (int i = 0;i < 100; i++)
		node->remove_key(100 + i);
	node->print();
#endif

#if _TEST_TREE_
	btree* bt = new btree(opt.fanout);

	auto t1 = std::chrono::high_resolution_clock::now();
	for (int i = 1;i <= opt.nr_keys; i++) {
		bt->_insert(i, value_t(NULL, 1000));
	}
	auto t2 = std::chrono::high_resolution_clock::now();
	bt->_print();
	std::chrono::duration<double, std::milli> ins_fp_ms = t2 - t1;
#if _TEST_DELETE_

	auto t3 = std::chrono::high_resolution_clock::now();
#if _FORWARD_DELETE
	for (int i = 1;i <= opt.nr_keys; i++) {
#elif _REVERSE_DELETE
	for (int i = opt.nr_keys;i >= 0; i--) {
#endif
		bt->_delete(i);
	}
	auto t4 = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::milli> del_fp_ms = t4 - t3;
#endif
	bt->_print();
 	delete bt;
#endif

#if _TEST_BPTREE_
	bptree* bt = new bptree(opt.fanout);
	for (int i = 1;i <= opt.nr_keys; i++)
	    bt->insert(i, mapping_t(NULL, 1000, 32));
	bt->print();
        bt->stats();
 	delete bt;
#endif        

#if 0
	cout << "################## Printing B-Tree Stats #####################" << endl;
	cout << " insert : " << ins_fp_ms.count() << " msecs " << endl;
	cout << " delete : " << del_fp_ms.count() << " msecs " << endl;
	return 0;
#endif
}

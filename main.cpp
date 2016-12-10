/*-------------------------------------------------------
 *
 *  g++ -std=c++11 -o main main.cpp btnode.cpp btree.cpp
 *
 * ------------------------------------------------------*/
#include <iostream>
#include <chrono>
#include <errno.h>
#include <unistd.h>
#include <getopt.h>

#include "btnode.h"
#include "btree.h"

#include "bptnode.h"
#include "bptree.h"

#include "boost_logger.h"

#define _TEST_NODE_ 0
#define _TEST_BPNODE_ 0
#define _TEST_BPINTERNALNODE_ 0
#define _TEST_BPTREE_ 1
#define _FORWARD_DELETE_BPTREE 0
#define _REVERSE_DELETE_BPTREE 1
#define _TEST_DELETE_BPTREE 1
#define _TEST_TREE_ 0
#define _TEST_DELETE_ 1
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

        static struct option long_options[] = {

            {"branch", required_argument, 0, 'b'},

            {"nkey", required_argument, 0, 'n'},

            {"file", required_argument, 0, 'f'}
        };

        int opt_index = 0;

	while ((c = getopt_long(argc, argv, "b:n:",
                        long_options, &opt_index)) != -1) {

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
            cerr << "Usage : [-b] fanout [-n] nkey" << endl;
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

        // Set-Up Boost Logging
        init_boost_logger();

#if _TEST_NODE_

	shared_ptr<btnode> node (new btnode());
	for (int i = 0;i < 3; i++)
		node->_insert_key(100 + i, value_t(NULL, 1000));

	node->_insert_key(99, value_t(NULL, 1000));

	int pos = node->_find_key(102);
	auto v = node->_keysAt(pos);

	node->_print();
        BOOST_LOG_TRIVIAL(info) << "Pos " << pos
                                << "Value " << v.second._off;
	                        << "Keys Size " << node->_num_keys();

	for (int i = 0;i < 100; i++)
		node->_remove_key(100 + i);

	node->_print();
#endif

#if _TEST_BPNODE_

	auto node = blkptr_leaf_t 
            (new bptnode_leaf(blkptr_internal_t(nullptr), 0));
	for (int i = 0;i < 3; i++)
		node->insert_record(100 + i, mapping_t(NULL, 1000, 32));

	node->insert_record(99, mapping_t(NULL, 1000, 32));

	int pos = node->find_key(102);
	auto v = node->_keysAt(pos);

	node->_print();
        BOOST_LOG_TRIVIAL(info) << "Pos " << pos
                                << "Value " << v.second._off;
	                        << "Keys Size " << node->_num_keys();

	for (int i = 0;i < 100; i++)
		node->remove_key(100 + i);

	node->print();
#endif

#if _TEST_BPINTERNALNODE_

	auto node = blkptr_internal_t 
            (new bptnode_internal(blkptr_internal_t(nullptr), 0));

	for (int i = 0;i < 3; i++)
		node->insert_key(100 + i);

	node->insert_key(99);

	int pos = node->find_key(102);
	auto v = node->_keysAt(pos);

	node->_print();
        BOOST_LOG_TRIVIAL(info) << "Pos " << pos
                                << "Value " << v.second._off;
	                        << "Keys Size " << node->_num_keys();

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
	auto t1 = std::chrono::high_resolution_clock::now();
	for (int i = 1;i <= opt.nr_keys; i++)
	    bt->insert(i, mapping_t(NULL, 1000, 32));
	auto t2 = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::milli> ins_fp_ms = t2 - t1;
	bt->print();
        bt->stats();
        
        BOOST_LOG_TRIVIAL(info) << "insert : " << ins_fp_ms.count() << " msecs";
#endif

#if _TEST_DELETE_BPTREE

#if _FORWARD_DELETE_BPTREE
	auto t3 = std::chrono::high_resolution_clock::now();

	for (int i = 1;i <= opt.nr_keys; i++) {
#elif _REVERSE_DELETE_BPTREE
	auto t3 = std::chrono::high_resolution_clock::now();

	for (int i = opt.nr_keys;i >= 0; i--) {
#endif
		bt->remove(i);
	}
	auto t4 = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::milli> del_fp_ms = t4 - t3;
        BOOST_LOG_TRIVIAL(info) << "delete : " << del_fp_ms.count() << " msecs";
#endif

#if 0
	cout << "################## Printing B-Tree Stats #####################" << endl;
	cout << " insert : " << ins_fp_ms.count() << " msecs " << endl;
	cout << " delete : " << del_fp_ms.count() << " msecs " << endl;
	return 0;
#endif
}

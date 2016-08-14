#include <iostream>

#include "btnode.h"
#include "btree.h"

#define _TEST_NODE_ 0
#define _TEST_TREE_ 1

int main(void) {
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
	btree* bt = new btree(3);
	for (int i = 0;i < 500; i++)
		bt->_insert(100 + i, value_t(NULL, 1000));
	cout << "################## Printing Tree #####################" << endl;
	bt->_print();
 	delete bt;
#endif
	return 0;
}

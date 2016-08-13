#include <iostream>

#include "btnode.h"
#include "btree.h"

#define _TEST_NODE_ 0
#define _TEST_TREE_ 1

int main(void) {
#if _TEST_NODE_

	btnode* node = new btnode(NULL, 0, 6);
	for (int i = 0;i < 3; i++)
		node->_insert_key(100 + i, value_t(NULL, 1000));
	node->_insert_key(99, value_t(NULL, 1000));
	node->_print();
	int pos = node->_findpos(102);
	cout << "Pos " << pos << endl;
	value_t v = node->_index(pos);
	cout << "Value " << v._off << endl;
	cout << "Keys Size " << node->_num_keys() << endl;
	for (int i = 0;i < 100; i++)
		cout << node->_delete(100 + i) << endl;
	node->_print();
	delete node;
#endif

#if _TEST_TREE_
	btree* bt = new btree(3);
	for (int i = 0;i < 250; i++)
		bt->_insert(100 + i, value_t(NULL, 1000));
	cout << "################## Printing Tree #####################" << endl;
	bt->_print();
 	delete bt;
#endif
	return 0;
}

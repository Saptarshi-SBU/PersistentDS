#include <iostream>

#include "btnode.h"

int main(void) {
	btnode* node = new btnode(NULL, 0, 6);
	for (int i = 0;i < 3; i++)
		cout << node->_insert(100 + i, value_t(NULL, 1000)) << endl;
	cout << node->_insert(99, value_t(NULL, 1000)) << endl;
	node->_print();
	int pos = node->_findpos(102);
	cout << "Pos " << pos << endl;
	value_t v = node->_index(pos);
	cout << "Value " << v._off << endl;
	for (int i = 0;i < 10; i++)
		cout << node->_delete(100 + i) << endl;
	node->_print();
	delete node;
	return 0;
}

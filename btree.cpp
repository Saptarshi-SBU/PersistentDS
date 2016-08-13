#include <iostream>
#include <cassert>
#include "btree.h"

using namespace std;

#define TRACE_FUNC(str) cout  << endl << str << " : "

btree::btree(int max) : _max_children(max) {

	try {
		_rootp = new btnode(NULL, 0, _max_children);
	} catch (exception& ex) {
		cout << ex.what() << endl;
		throw ex;
	}
}

btree::~btree() {

	if (_rootp != NULL) {
		_destroy(_rootp);
		_rootp = NULL;
	}
}

void btree::_destroy(btnode* node) {

	if (node == NULL)
		return;

	for (auto i = 0; i < node->_num_child(); i++)
		_destroy(node->_childAt(i));
}

btnode* btree::_find_leaf(const bkey_t key, btnode* nodep) {

	btnode* rnode = NULL;	

	if (nodep == NULL)
		return NULL;

	TRACE_FUNC(__func__);

	cout << "keys : " << nodep->_num_keys() << "\tchilds : " << nodep->_num_child() << " min : " << nodep->_min << " max : " << nodep->_max << endl;

	if (0 == nodep->_num_child())	
		return nodep;

	int i  = 0;
	for (i = 0; i < nodep->_num_keys(); i++) {
		auto p = nodep->_keysAt(i);
		if (key > p.first)
			continue;
		rnode = nodep->_childAt(i);
		break;
	}

	if (NULL == rnode)
		rnode = nodep->_childAt(i);


	return _find_leaf(key, rnode);
}

void btree::_do_split(btnode* node) {

	assert((NULL != node) && (node->_num_keys() >= _max_children));

	btnode* parentp = node->_parentp();

	TRACE_FUNC(__func__);

	cout << "split node refcnt " << node->_refcnt << " num child " << node->_num_child() << endl;
	
	if (NULL == parentp)
		cout << "no parentp " << endl; 
	else
		cout << "parent node refcnt " << parentp->_refcnt << " num child " << parentp->_num_child() << " max " << parentp->_max << " min " << parentp->_min << endl;
		
	if (NULL == parentp)
		parentp = new btnode(NULL, node->_num_level() - 1, _max_children);
	else
		cout << "delete child " << parentp->_unset_child(node) << endl;

	btnode* lchildp = new btnode(parentp, node->_num_level(), _max_children);

	btnode* rchildp = new btnode(parentp, node->_num_level(), _max_children);

	int  p = node->_separator();

	auto q = node->_keysAt(p);

	for (int i = 0; i < p; i++)
		lchildp->_insert_key(node->_keysAt(i).first, node->_keysAt(i).second);

	for (int i = p + 1; i < node->_num_keys(); i++)
		rchildp->_insert_key(node->_keysAt(i).first, node->_keysAt(i).second);

	if (node->_num_child()) {

		for (int i = 0; i <= p; i++) {
			assert(node->_childAt(i) != NULL);
			lchildp->_insert_child(node->_childAt(i));
		}

		for (int i = p + 1; i < node->_num_child(); i++) {
			assert(node->_childAt(i) != NULL);
			rchildp->_insert_child(node->_childAt(i));
		}
	}

	parentp->_insert_key(q.first, q.second);

//	parentp->_insert_child(lchildp);

//	parentp->_insert_child(rchildp);

	if (_rootp == node)
		_rootp = parentp;


	delete node;

	cout << "parent info " << parentp->_num_keys() << "\t" << parentp->_num_child() << endl;

	_do_print(parentp);

	if (parentp->_num_keys() >= _max_children)
		_do_split(parentp);
}

void btree::_do_insert(const bkey_t key, value_t val, btnode* node) {

	if (node == NULL)
		return;

	TRACE_FUNC(__func__);

	cout << "key :" << key << endl;

	btnode* rnode = _find_leaf(key, node);
	assert (NULL != rnode);

	rnode->_insert_key(key, val);

	if (rnode->_num_keys() >= _max_children)
		_do_split(rnode);
}

void btree::_insert(const bkey_t key, const value_t val) {

	_do_insert(key, val, _rootp);	
}

void btree::_do_print(btnode* node) const {

	if (node == NULL)
		return;

	TRACE_FUNC(__func__);

	for (auto i = 0; i < node->_num_child(); i++) 
		_do_print(node->_childAt(i));

	node->_print();
}

void btree::_print(void) const {

	_do_print(_rootp);
}

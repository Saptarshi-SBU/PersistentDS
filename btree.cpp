#include <iostream>
#include <cassert>
#include "btree.h"

using namespace std;

#define TRACE_FUNC(str) cout  << endl << str <<  endl

#define DEBUG 1

btree::btree(int max) : _max_children(max) {

	try {
		_rootp = shared_ptr<btnode> (new btnode());
	} catch (exception& ex) {
		throw ex;
	}
}

btree::~btree() {

	_destroy(_rootp);
}

void btree::_destroy(shared_ptr<btnode> node) {

	for (auto i = 0; i < node->_num_child(); i++) {
		 node->_childAt(i).reset();
		_destroy(node->_childAt(i));
	}

	node->_reset_parentp();
	node.reset();
}

shared_ptr<btnode> btree::_locate_leaf(const bkey_t key, shared_ptr<btnode>& node) {

	shared_ptr<btnode> rnode;	

	if (!node)
		return shared_ptr<btnode>();

	if (!(node->_num_child()))	
		return node;

	for (int i = 0; i < node->_num_child(); i++) {
		rnode = node->_childAt(i);
		if (key > rnode->_max)
			continue;
		break;
	}

	return _locate_leaf(key, rnode);
}

void btree::_do_split(shared_ptr<btnode>& node) {

	assert((node) && (node->_num_keys() >= _max_children));

	auto p = node->_separator();

	auto q = node->_keysAt(p);

	auto lchildp = shared_ptr<btnode> (new btnode());

	auto rchildp = shared_ptr<btnode> (new btnode());

	for (int i = 0; i < p; i++)
		lchildp->_insert_key(node->_keysAt(i).first, node->_keysAt(i).second);

	for (int i = p + 1; i < node->_num_keys(); i++)
		rchildp->_insert_key(node->_keysAt(i).first, node->_keysAt(i).second);

	if (node->_num_child()) {

		for (int i = 0; i <= p; i++) {
			auto r = node->_childAt(i);
			assert(r);
			lchildp->_insert_child(r);
			r->_set_parentp(lchildp);
		}

		for (int i = p + 1; i < node->_num_child(); i++) {
			auto r = node->_childAt(i);
			assert(r);
			rchildp->_insert_child(r);
			r->_set_parentp(rchildp);
		}
	}

	auto parentp = node->_parentp();

	if (!parentp) {
		parentp = shared_ptr<btnode> (new btnode()); 
		parentp->_set_level(node->_get_level() - 1);
	}

	parentp->_insert_key(q.first, q.second);

	parentp->_remove_child(node);

	parentp->_insert_child(lchildp);

	parentp->_insert_child(rchildp);

	// Note No calls to unset parent required here
	
	lchildp->_set_parentp(parentp);

	rchildp->_set_parentp(parentp);

	lchildp->_set_level(node->_get_level());

	rchildp->_set_level(node->_get_level());

	if (_rootp == node)
		_rootp = parentp;

	if (parentp->_num_keys() >= _max_children)
		_do_split(parentp);

	#if DEBUG
	cout << "btnode ref count " << node.use_count() << endl;
	#endif

	node.reset();
}

void btree::_do_insert(const bkey_t key, value_t val, shared_ptr<btnode>& node) {

	TRACE_FUNC(__func__);

	if (!node)
		return;

	auto rnode = _locate_leaf(key, node);

	assert (rnode);

	rnode->_insert_key(key, val);

	if (rnode->_num_keys() >= _max_children)
		_do_split(rnode);
}

void btree::_insert(const bkey_t key, const value_t val) {

	_do_insert(key, val, _rootp);	
}

void btree::_do_print(const shared_ptr<btnode>& node) const {

	TRACE_FUNC(__func__);

	if (!node)
		return;

	int num_c = node->_num_child();
	int num_k = node->_num_keys();

	if (!num_c) {
		for (auto i = 0; i < num_k; i++)
		 	cout << "btkey : " << node->_keysAt(i).first << endl;
	} else {
		for (auto i = 0; i < num_c; i++) {
			_do_print(node->_childAt(i));
		 	if (i < num_k)
		 		cout << "btkey : " << node->_keysAt(i).first << endl;
		}
	}
	node->_print();
}

void btree::_print(void) const {

	_do_print(_rootp);
}

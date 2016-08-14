#include <iostream>
#include <cassert>
#include <algorithm>
#include <stdexcept>

#include "btnode.h"

#define LOG cout

/*
 *  B-Tree node constructor
 * 
 */
btnode::btnode(void) : _parent(NULL), _level(0) { }

/*
 *  B-Tree node destructor
 * 
 */
btnode::~btnode() {
//	 LOG << FUNC << KEY_LIMITS(_min, _max) << ENTRY_LIMITS(_num_keys(), _num_child()) << endl;
	_keys.clear();
	_child.clear();
}

/*
 *  B-Tree node GET functions
 * 
 */
int btnode::_num_keys(void) const {
	return _keys.size();
}

int btnode::_num_child(void) const {
	return _child.size();
}

int btnode::_num_level(void) const {
	return _level;
}

void btnode::_set_level(int level) {
	_level = level;
}

int btnode::_get_level(void) const {
	return _level;
}

/*
 *  B-Tree node Link parent
 * 
 */
void btnode::_set_parentp(shared_ptr<btnode> node) {
	_parent = node;
}

/*
 *  B-Tree node UnLink parent
 * 
 */
void btnode::_reset_parentp(void) {
	_parent.reset();
}

/*
 *  B-Tree node insert key operation
 * 
 */
void btnode::_insert_key(const bkey_t key, const value_t p) {
	 LOG << FUNC << endl;
	_keys.push_back(make_pair(key, p));
	 sort(_keys.begin(), _keys.end(), [] (const element_t& p, const element_t& q) { return (p.first < q.first);} );
	_min = _keys.begin()->first;
	_max = _keys.rbegin()->first;
}

/*
 *  B-Tree node lookup key operation
 * 
 */
int btnode::_find_key(const bkey_t key) const {
	auto it = std::find_if(_keys.begin(), _keys.end(), [key] (const element_t p) { return p.first == key;});
	return (it != _keys.end()) ? std::distance(_keys.begin(), it) : -1;
}

/*
 *  B-Tree node remove key operation
 * 
 */
void btnode::_remove_key(const bkey_t key) {
	 LOG << FUNC << endl;
	_keys.erase(std::remove_if(_keys.begin(), _keys.end(), [key] (const element_t& p) { return p.first == key; }), _keys.end());
}

/*
 *  B-Tree node insert ptr operation
 * 
 */
void btnode::_insert_child(shared_ptr<btnode> node) {
	 LOG << FUNC << endl;
	_child.push_back(node);
//	 node->_reset_parentp();
//	 node->_set_parentp(shared_ptr<btnode>(this));
//	 node->_set_parentp(shared_from_this());
	 sort(_child.begin(), _child.end(), [] (const shared_ptr<btnode> p, const shared_ptr<btnode> q) { return (p->_max < q->_max);} );
}

/*
 *  B-Tree node lookup key operation
 * 
 */
int btnode::_find_child(shared_ptr<btnode>& node) const {
	auto it = std::find(_child.begin(), _child.end(), node);
	return (it != _child.end()) ? std::distance(_child.begin(), it) : -1;
}

/*
 *  B-Tree node remove ptr operation
 * 
 */
void btnode::_remove_child(shared_ptr<btnode>& node) {
	 LOG << FUNC << endl;
	_child.erase(std::remove(_child.begin(), _child.end(), node), _child.end());
	 node->_reset_parentp();
}

/*
 *  B-Tree node GET parent
 * 
 */
shared_ptr<btnode> btnode::_parentp(void) const {
	return _parent;
}

/*
 *  B-Tree node GET Key
 * 
 */
element_t btnode::_keysAt(int no) const {
	if (no >= _num_keys())
		throw exception();
	return _keys.at(no);
}

/*
 *  B-Tree node GET child ptr
 * 
 */
shared_ptr<btnode> btnode::_childAt(int no) {
	if (no >= _num_child())
		throw exception();
	return _child.at(no);
}

/*
 *  B-Tree node GET median
 * 
 */
int btnode::_separator(void) const {
	return _keys.size()/2;
}

/*
 *  B-Tree node print node information
 * 
 */
void btnode::_print(void) const {
	LOG << FUNC << endl;
	LOG << LEVEL(_level) << KEY_LIMITS(_min, _max) << ENTRY_LIMITS(_num_keys(), _num_child()) << endl;
#if 0
	LOG << "key_list  : " ;
	for (auto& i : _keys)
		LOG << i.first << "  ";
	LOG << "child_list  : " ;
	for (auto& i : _child)
		LOG << ENTRY_LIMITS(i->_min, i->_max);
	LOG << endl;
#endif
}

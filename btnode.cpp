#include <iostream>
#include <cassert>
#include <algorithm>

#include "btnode.h"

btnode::btnode(btnode* parent, int level, int keys) : _level(level), _parent(parent) { 
	_refcnt = 1; 
	if (NULL != parent) {
		parent->_child.push_back(this);
	       _set_parentp(parent);
		sort(parent->_child.begin(), parent->_child.end(), [] (const btnode* p, const btnode* q) { return (p->_max < q->_max);} );
	}
}

btnode::~btnode() {
	cout << __func__ << " min : " << _min << " max : " << _max << " num keys " << _num_keys() << " num child " << _num_child() << endl;
	--_refcnt;
	assert(_refcnt == 0);
	_parent = NULL;
	_keys.clear();
	_child.clear();
}

void btnode::_set_parentp(btnode* node) {

	_parent = node;
	_parent->_refcnt++;
	_refcnt++;
}

void btnode::_unset_parentp(void) {

	if (NULL == _parent)
		return;
	_parent->_refcnt--;
	_parent = NULL;
	_refcnt--;
}

void btnode::_insert_key(const bkey_t key, const value_t p) {

	_keys.push_back(make_pair(key, p));

	sort(_keys.begin(), _keys.end(), [] (const element_t& p, const element_t& q) { return (p.first < q.first);} );

	_min = _keys.begin()->first;

	_max = _keys.rbegin()->first;
}

void btnode::_insert_child(btnode *node) {

	_child.push_back(node);

	node->_unset_parentp();

	node->_set_parentp(this);

	cout << __func__ << " " << _refcnt << endl;

	sort(_child.begin(), _child.end(), [] (const btnode* p, const btnode* q) { return (p->_max < q->_max);} );
}

int btnode::_findpos(const bkey_t key) const {
	auto it = std::find_if(_keys.begin(), _keys.end(), [key] (const element_t p) { return p.first == key;});
	if (it != _keys.end())
		return std::distance(_keys.begin(), it);
	return -1;	
}

int btnode::_delete(const bkey_t key) {
	int n = _keys.size();
	if (n > 0)
		_keys.erase(std::remove_if(_keys.begin(), _keys.end(), [key] (const element_t& p) { return p.first == key; }), _keys.end());
	return (n > _keys.size()) ? 0 : -1;
}

int btnode::_unset_child(btnode* node) {
	int n = _child.size();

	if (n > 0)
		_child.erase(std::remove(_child.begin(), _child.end(), node), _child.end());
	node->_unset_parentp();
	cout << __func__ << " " << node->_refcnt << endl;
	//	_child.erase(std::remove_if(_child.begin(), _child.end(), [node] (const btnode* p) { return p == node; }), _child.end());
	return (n > _child.size()) ? 0 : -1;
}

int btnode::_separator(void) const {
	return _keys.size()/2;
}

value_t btnode::_index(int i) const {
	if (i <= _keys.size())
		return _keys.at(i).second;
	else
		return value_t(NULL, 0);
}

element_t btnode::_keysAt(int no) const {
	if (no >= _num_keys())
		return make_pair(0, value_t(NULL, 0));
	return _keys.at(no);
}

btnode* btnode::_childAt(int no) {
	if (no >= _num_child())
		return NULL;
	return _child.at(no);
}

btnode* btnode::_parentp(void) const {
	return _parent;
}

int btnode::_num_keys(void) const {
	return _keys.size();
}

int btnode::_num_child(void) const {
	return _child.size();
}

int btnode::_num_level(void) const {
	return _level;
}

void btnode::_print(void) const {
	cout << __func__   << " ";
	cout << "level : " << _level << " nchild : " << _num_child() << " nkeys : " << _num_keys() << " ";
	cout << "key_list  : " ;
	for (auto& i : _keys)
		cout << i.first << "  ";
	cout << "child_list  : " ;
	for (auto& i : _child)
		cout << "<" << i->_min << "," << i->_max << ">" << " ";
	cout << endl;
}

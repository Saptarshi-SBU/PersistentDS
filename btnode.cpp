#include <iostream>
#include <algorithm>

#include "btnode.h"

btnode::btnode(btnode* parent, int level, int keys) : _num_keys(keys), _level(level), _parent(parent) { }

btnode::~btnode() {

	_keys.clear();
	_child.clear();
}

int btnode::_insert(const bkey_t key, const value_t p) {

	int n = _keys.size();
	
	if (n >= _num_keys)
		return -1;

	_keys.push_back(make_pair(key, p));

	sort(_keys.begin(), _keys.end(), [] (const element_t& p, const element_t& q) { return (p.first < q.first);} );

	return 0;
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

int btnode::_separator(void) const {
	return _keys.size()/2;
}

value_t btnode::_index(int i) const {
	if (i <= _keys.size())
		return _keys.at(i).second;
	else
		return value_t(NULL, 0);
}

void btnode::_print(void) const {

	cout << "printing keys " << endl;
	cout << "Level : " << _level << endl;
	for (auto& i : _keys)
		cout << i.first << endl;
}

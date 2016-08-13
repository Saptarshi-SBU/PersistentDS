#ifndef _BTNODE_H
#define _BTNODE_H

#include <vector>
#include <utility>

using namespace std;

typedef unsigned long long bkey_t;

struct value_t {
		void* _base;

		unsigned long long _off;

		value_t(void* base, unsigned long long offset) : _base(base), _off(offset) { }

	       ~value_t() { }
};

typedef struct value_t value_t;

typedef pair<bkey_t, value_t> element_t;
		
class btnode {

	private:

		vector<element_t> _keys;

		vector<btnode*> _child;

		btnode* _parent;

		int _level;


	protected:

	public:

		int _refcnt;
		void _insert_key(const bkey_t, const value_t);

		void _insert_child(btnode*);

		int _findpos(const bkey_t) const;

		int _delete(bkey_t);

		int _separator(void) const;

		value_t _index(int) const;

		element_t _keysAt(int) const;

		btnode* _childAt(int);

		btnode* _parentp(void) const;

		void _set_parentp(btnode *);

		void _unset_parentp(void);

		int _unset_child(btnode *);

		int _num_keys(void) const;

		int _num_child(void) const;

		int _num_level(void) const;

		void _print(void) const;

		btnode(btnode*, int, int);

	       ~btnode();

		bkey_t _max, _min;
};
#endif

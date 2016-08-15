#ifndef _BTNODE_H
#define _BTNODE_H

#include <vector>
#include <utility>
#include <memory>

using namespace std;

#define FUNC                __func__
#define KEY_LIMITS(X, Y)   "key_limits<" << X << "," << Y << ">" << " "
#define ENTRY_LIMITS(X, Y) "entry_limits<" << X << "," << Y << ">" << " "
#define LEVEL(X)           "level<" << X << ">" << " "

typedef unsigned long long bkey_t;

struct value_t {
		void* _base;

		unsigned long long _off;

		value_t(void* base, unsigned long long offset) : _base(base), _off(offset) { }

		value_t() { }

	       ~value_t() { }
};

typedef struct value_t value_t;

typedef pair<bkey_t, value_t> element_t;
		
class btnode : public enable_shared_from_this<btnode> {

	private:

		vector<element_t> _keys;

		vector<shared_ptr<btnode>> _child;

		shared_ptr<btnode> _parent;

		int _level;

	protected:

	public:

		bkey_t _max, _min;

		btnode();

	       ~btnode();

		void _insert_key(const bkey_t, const value_t);

		int _find_key(const bkey_t) const;

		void _remove_key(const bkey_t);

		void _insert_child(shared_ptr<btnode>);

		int _find_child(shared_ptr<btnode>&) const;

		void _remove_child(shared_ptr<btnode>);

		int _separator(void) const;

		element_t _keysAt(int) const;

		shared_ptr<btnode> _childAt(int);

		shared_ptr<btnode> _parentp(void) const;

		void _set_parentp(shared_ptr<btnode>);

		void _reset_parentp(void);

		int _num_keys(void) const;

		int _num_child(void) const;

		int _num_level(void) const;

		void _set_level(int);

		int _get_level(void) const;

		void _print(void) const;


};
#endif

#ifndef _BTREE_H
#define _BTREE_H

#include <memory>

#include "btnode.h"

class btree {

	private:

		shared_ptr<btnode> _rootp;

		int _max_children;
	
		void _do_split(shared_ptr<btnode>&);

		void _do_insert(const bkey_t, const value_t, shared_ptr<btnode>&);

		void _do_print(const shared_ptr<btnode>&) const;
	
		shared_ptr<btnode> _locate_leaf(const bkey_t key, shared_ptr<btnode>&);

	public:
		
		void _insert(const bkey_t, const value_t);

		void _print() const;

		void _destroy(shared_ptr<btnode>);

		btree(int);

	       ~btree();
};
#endif

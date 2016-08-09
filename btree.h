#ifndef _BTREE_H
#define _BTREE_H

#include "btnode.h"

class btree {

	private:

		btnode* _rootp;

		int _max_children;
	
		void _do_split(btnode* node);

		void _do_insert(const bkey_t, const value_t, btnode *);

		void _do_print(btnode*) const;
	
		btnode* _find_leaf(const bkey_t key, btnode* nodep);

	public:
		
		void _insert(const bkey_t, const value_t);

		void _print() const;

		void _destroy(btnode*);

		btree(int);

	       ~btree();
};
#endif

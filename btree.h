#ifndef _BTREE_H
#define _BTREE_H

#include "btnode.h"

typedef unsigned long long key_t;

class btree {

	private:

		btnode* _rootp;

		int _max_children;
	
	protected:
	
		int _height(void);

		void _create(void);

		bool _search(key_t key);

		void _insert(btnode&);

		void _splitnode(bnode&);

		int _median(btnode&);

		void _delete(key_t key);

		void _merge(btnode&);

	public:

		btree(int);
	       ~btree();
};
#endif

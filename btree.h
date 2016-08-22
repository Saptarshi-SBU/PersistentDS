#ifndef _BTREE_H
#define _BTREE_H

#include <memory>

#include "btnode.h"

class btree {

	private:

		shared_ptr<btnode> _rootp;

		int _max_children;
	
		void _do_split(shared_ptr<btnode>&);

		shared_ptr<btnode> _do_merge(shared_ptr<btnode>&, shared_ptr<btnode>&, shared_ptr<btnode>&);

		bool _test_merge(shared_ptr<btnode>&, shared_ptr<btnode>&, shared_ptr<btnode>&);

		void _do_right_rotation(shared_ptr<btnode>&, shared_ptr<btnode>&, shared_ptr<btnode>&);

		void _do_left_rotation(shared_ptr<btnode>&, shared_ptr<btnode>&, shared_ptr<btnode>&);

		void _do_insert(const bkey_t, const value_t, shared_ptr<btnode>&);

		shared_ptr<btnode> _do_lookup(const bkey_t key, shared_ptr<btnode> node);

		shared_ptr<btnode> _inorder_predecessor(const bkey_t key);

		void _do_print(const shared_ptr<btnode>&) const;

		void _do_level_traversal(const shared_ptr<btnode>& node) const;
	
		shared_ptr<btnode> _locate_leaf(const bkey_t key, shared_ptr<btnode>&);

		bool _test_valid_leaf(shared_ptr<btnode> node);

		bool _test_valid_non_leaf(shared_ptr<btnode> node);
	public:
		
		void _insert(const bkey_t, const value_t);

		shared_ptr<btnode> _lookup(const bkey_t);

		void _delete(const bkey_t);

		void _print() const;

		void _destroy(shared_ptr<btnode>);

		btree(int);

	       ~btree();
};
#endif

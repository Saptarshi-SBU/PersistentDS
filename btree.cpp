/*--------------------------------------------------------------------------------
 *   B-Tree Properties
 *	- Every node has at most m children.
 *	- Every non-leaf node (except root) has at least ⌈m/2⌉ children.
 *	- The root has at least two children if it is not a leaf node.
 *	- A non-leaf node with k children contains k−1 keys.
 *	- All leaves appear in the same level
 *---------------------------------------------------------------------------------*/


#include <iostream>
#include <cassert>
#include <queue>
#include "btree.h"

using namespace std;

#define TRACE_FUNC(str) 1 //cout  << endl << str <<  endl

#define DEBUG 1

btree::btree(int max) : _max_children(max) {

	try {
		if (max < 3)
			throw exception();
		else
                       _rootp = shared_ptr<btnode> (new btnode());
                       _k =(max % 2) ? (max/2) + 1 : (max/2);
	} catch (exception& ex) {
		throw ex;
	}
}

btree::~btree() {

	if (_rootp != nullptr)
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

/*
 *  B-Tree Merge nodes operation 
 *
 */
shared_ptr<btnode> btree::_do_merge(shared_ptr<btnode>& parentp, shared_ptr<btnode>& left, shared_ptr<btnode>& right) {

	//Sanity Check that we have no siblings to avoid merge
        TRACE_FUNC(__func__);
	
	assert(left && (left->_num_keys() <= (_k - 1)));

	assert(right && (right->_num_keys() <= (_k - 1)));

	assert(parentp && (parentp->_num_child() >= 2));

	auto merge_node = shared_ptr<btnode> (new btnode());

	auto merge_level = left->_get_level();
	
	merge_node->_set_level(merge_level);

	cout << "Merge : left-max " << left->_max << " right-min " << right->_min << "parentp min-max " << parentp->_min << ":" << parentp->_max << endl;

        for (int i = 0; i < parentp->_num_child(); i++) {
		if (parentp->_childAt(i) == left) {
			auto key = parentp->_keysAt(i);
			merge_node->_insert_key(key.first, key.second);
			parentp->_remove_key(key.first);
			cout << "Merge : key removed from parent " << key.first << endl;
			break;
		}	
	}

	for (int i = 0; i < left->_num_keys(); i++)
		merge_node->_insert_key(left->_keysAt(i).first, left->_keysAt(i).second);
	
	for (int i = 0; i < right->_num_keys(); i++)
		merge_node->_insert_key(right->_keysAt(i).first, right->_keysAt(i).second);

	for (int i = 0; i < left->_num_child(); i++) {
		auto r = left->_childAt(i);
		assert(r);
		merge_node->_insert_child(r);
		r->_set_parentp(merge_node);
	}

	for (int i = 0; i < right->_num_child(); i++) {
		auto r = right->_childAt(i);
		assert(r);
		merge_node->_insert_child(r);
		r->_set_parentp(merge_node);
	}
	
	parentp->_remove_child(left);

	parentp->_remove_child(right);

	parentp->_insert_child(merge_node);

	merge_node->_set_parentp(parentp);

	cout << "Merge Node : ";

        merge_node->_print();

	left.reset();

	right.reset();

	return parentp;
}

/*@
 *  This assumes both the left and right child are leaf nodes
 *  Clock-Wise Rotation
 */
void btree::_do_right_rotation(shared_ptr<btnode>& curr, shared_ptr<btnode>& parentp, shared_ptr<btnode>& left) {

	TRACE_FUNC(__func__);

	assert (curr && (curr->_num_keys() < (_k - 1)));

	//assert (parentp && (parentp->_num_keys() >= _k));

	assert (left && (left->_num_keys() >= _k));

	element_t mid_key;

	for (int i = 0; i < parentp->_num_keys(); i++) {
		if (parentp->_keysAt(i).first < left->_max)
			mid_key = parentp->_keysAt(i);
		else
			break;
	}

	parentp->_remove_key(mid_key.first);

	curr->_insert_key(mid_key.first, mid_key.second);

	// in-order predecessor in left child to parent node for RL-rotation
	 
	int max_index = left->_find_key(left->_max);

	element_t lkey = left->_keysAt(max_index);

	left->_remove_key(lkey.first);

	parentp->_insert_key(lkey.first, lkey.second);

	if (left->_isLeaf() == false) {

		auto sibling = left->_childAt(max_index + 1);

		curr->_insert_child(sibling);

		left->_remove_child(sibling);

	} else {
		assert (curr->_isLeaf());
	}
}

/*
 *  Anti-clockwise rotation 
 */
void btree::_do_left_rotation(shared_ptr<btnode>& curr, shared_ptr<btnode>& parentp, shared_ptr<btnode>& right) {

	TRACE_FUNC(__func__);

	assert(curr && (curr->_num_keys() < (_k - 1)));

	assert(right && (right->_num_keys() >= _k));

	//assert(parentp && (parentp->_num_keys() >= _k));

	element_t replace_key;

	for (int i = 0; i < parentp->_num_keys(); i++) {
	        auto key = parentp->_keysAt(i);
		if (key.first > right->_max)
			break;
                replace_key = key;
	}


	// Pick From Edges
	
	int min_index = right->_find_key(right->_min);

	assert(min_index == 0);

	element_t rkey = right->_keysAt(min_index);

        // step :1
	parentp->_remove_key(replace_key.first);

        // step :2
	curr->_insert_key(replace_key.first, replace_key.second);

        //step :3 
	right->_remove_key(rkey.first);

        //step :4
	parentp->_insert_key(rkey.first, rkey.second);

        //step :5
       	if (right->_isLeaf() == false) {

		auto sib = right->_childAt(min_index);

		curr->_insert_child(sib);

		right->_remove_child(sib);
	} else
		assert (curr->_isLeaf());
}


shared_ptr<btnode> btree::_do_lookup(const bkey_t key, shared_ptr<btnode> node) {

        int i;

	TRACE_FUNC(__func__);

	assert (node);

	cout << "lookup node <" << node->_min << "," << node->_max << ">" << endl;

	for (int i = 0; i < node->_num_keys(); i++)
		if (key == node->_keysAt(i).first)
			return node;

        if (0 == node->_num_child())
        	return nullptr;

	for (i = 0; i < node->_num_keys(); i++)
		if (key < node->_keysAt(i).first)
                       return _do_lookup(key, node->_childAt(i));

        return _do_lookup(key, node->_childAt(i));

}

shared_ptr<btnode> btree::_lookup(const bkey_t key) {

	TRACE_FUNC(__func__);

	return _do_lookup(key, _rootp);
}

/*
 *  This function drives the B-Tree Delete State Machine
 *
 */
bool btree::_test_valid_leaf(shared_ptr<btnode> node) {
	return ((node->_num_child() == 0) && node->_num_keys()) ? true : false;
} 

bool btree::_test_valid_non_leaf(shared_ptr<btnode> node) {
	return (node->_num_child() >= _k) ? true : false;
} 

/*
 * B-Tree Step 1 Operation during Delete
 */

void btree::_leaf_push2delete(bkey_t key, shared_ptr<btnode>& curr, shared_ptr<btnode>& leaf) {

        TRACE_FUNC(__func__);

	assert(leaf->_num_keys());

	assert(leaf->_num_child() == 0);

	if (leaf == curr) {
	        cout << "btkey deleted : " << key << "inorder leaf : " << leaf->_max << endl;
                curr->_remove_key(key);
	        cout << "num keys : " << curr->_num_keys() << endl;
		return;
	}


        //replace this key with that in the leaf. 
        //Ensure curr mantains ordering of child nodes

        int pos  = leaf->_find_key(leaf->_max);

	auto val = leaf->_keysAt(pos);

        assert(leaf->_max == val.first);

        curr->_insert_key(val.first, val.second);

        leaf->_remove_key(val.first);

        curr->_remove_key(key);

	cout << "btkey deleted : " << key << "inorder leaf : " << leaf->_max << endl;
}

/*
 * B-Tree Step 1 Operation during Delete
 */

shared_ptr<btnode> btree::_rebalance_tree(shared_ptr<btnode> curr) {

     int i = 0;

     TRACE_FUNC(__func__);

     assert (curr != nullptr);

     if (curr == _rootp)
	return curr;

     if ((curr->_isLeaf() && _test_valid_leaf(curr)) || (_test_valid_non_leaf(curr)))
	return curr;
       
     auto parentp = curr->_parentp();
     for (i = 0; i < parentp->_num_child(); i++)
	if (parentp->_childAt(i) == curr)
		break;
        
     assert (i < parentp->_num_child());

     shared_ptr<btnode> prev_sib, next_sib;

     try {
 	    prev_sib = parentp->_childAt(i - 1);
     } catch (exception e) {
	    prev_sib = nullptr;	
     }

     try {
     	    next_sib = parentp->_childAt(i + 1);
     } catch (exception e) {
            next_sib = nullptr;
     } 
	
     if (prev_sib && (prev_sib->_num_keys() >= _k)) {
      	_do_right_rotation(curr, parentp, prev_sib);
         return parentp;
     } else if (next_sib && (next_sib->_num_keys() >= _k)) {
	_do_left_rotation(curr, parentp, next_sib);
         return parentp;
     } else {
	 auto sib = prev_sib != nullptr ? prev_sib : next_sib;
         assert (sib != nullptr);
         curr = (sib == prev_sib) ? _do_merge(parentp, prev_sib, curr) : _do_merge(parentp, curr, next_sib);
	 return _rebalance_tree(curr);
     }
}

/*
 * B-Tree Delete is a recursive operation, which propagates towards the root
 * This may involve the following operations
 *    - Inorder Predecessor Finding
 *    - Swapping Keys
 *    - Merging Nodes in case B-Tree Property is violated
 *    - Rotation in case B-Tree Property is violated
 */     
void btree::_delete(const bkey_t key) {

	TRACE_FUNC(__func__);

	cout << "delete btkey " << key << endl; 
	
	if (_rootp == nullptr)
		return;

	auto curr = _lookup(key);
	if (!curr) {
		cout << "btkey " << key << " not found " << endl;
		return;
	}

	auto leaf = _inorder_predecessor(key);

	_leaf_push2delete(key, curr, leaf);

	auto node = _rebalance_tree(leaf);

	// No entries in Tree
	if ((node == _rootp) && (0 == node->_num_keys())) {
		_rootp.reset();
		if (node->_isLeaf() == false)
			_rootp = node->_childAt(0);
	}
}

/*
 * Note for leaf btree-node, we do not do any predecessor search
 * This is done only for the internal nodes
 *
 */
shared_ptr<btnode> btree::_inorder_predecessor(const bkey_t key) {

	TRACE_FUNC(__func__);

	auto node = _do_lookup(key, _rootp);
	if (!node)
	     // Sanity	
		return nullptr;
	     // Leaf node
	else if (0 == node->_num_child())	
		return node;
	     // Non-Leaf node
	else {
		int  index = node->_find_key(key);
		// in-order sibling
		auto child = node->_childAt(index);

		while(index = child->_num_child())	
			child = child->_childAt(index - 1);
		return child;
	}
}
		
void btree::_do_print(const shared_ptr<btnode>& node) const {

	TRACE_FUNC(__func__);

	if (!node)
		return;

	int num_c = node->_num_child();
	int num_k = node->_num_keys();

	if (!num_c) {
		for (auto i = 0; i < num_k; i++)
		 	cout << "inorder btkey : " << node->_keysAt(i).first << endl;
	} else {
		for (auto i = 0; i < num_c; i++) {
			_do_print(node->_childAt(i));
		 	if (i < num_k)
		 		cout << " inorder btkey : " << node->_keysAt(i).first << endl;
		}
	}
	node->_print();
}

void btree::_do_level_traversal(const shared_ptr<btnode>& node) const {

	if (!node)
		return;

	queue<shared_ptr<btnode>> q1, q2;
	shared_ptr<btnode> p;
	int n1, n2;

	q1.push(node);	

	while (q1.size() || q2.size()) {

		n1 = n2 = 0;

		while (q1.size()) {
			p = q1.front();

			q1.pop();
			
			for (int i = 0; i < p->_num_keys(); i++)
				cout << p->_keysAt(i).first << ", ";
			//cout << p->_keysAt(i).first << " ref :" << p.use_count() << " num child : " << p->_num_child() << ",";
			for (int i = 0; i < p->_num_child(); i++)
				q2.push(p->_childAt(i));

			cout << "<" << p->_num_child() << ">" << endl;
			n1++;
		} 

		cout << endl << "Level :" << p->_get_level() << " Nodes : " << n1 << endl;

		while (q2.size()) {
			p = q2.front();

			q2.pop();
			
			for (int i = 0; i < p->_num_keys(); i++)
				cout << p->_keysAt(i).first << ", ";
			//cout << p->_keysAt(i).first << " ref :" << p.use_count() << " num child : " << p->_num_child() << ",";
			for (int i = 0; i < p->_num_child(); i++)
				q1.push(p->_childAt(i));
		
			cout << "<" << p->_num_child() << ">" << endl;
			n2++;
		}

		cout << endl << "Level :" << p->_get_level() << " Nodes : " << n2 << endl;
	}
}

void btree::_print(void) const {

	 if (_rootp != nullptr) {
		_do_print(_rootp);
		_do_level_traversal(_rootp);
	 }
}

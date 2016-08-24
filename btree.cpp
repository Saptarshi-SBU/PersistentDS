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

#define TRACE_FUNC(str) cout  << endl << str <<  endl

#define DEBUG 1

btree::btree(int max) : _max_children(max) {

	try {
		_rootp = shared_ptr<btnode> (new btnode());
	} catch (exception& ex) {
		throw ex;
	}
}

btree::~btree() {

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
	
	assert(left && (left->_num_keys() <= _max_children/2));

	assert(right && (right->_num_keys() <= _max_children/2));

	assert(parentp && parentp->_num_child());

	auto merge_node  = shared_ptr<btnode> (new btnode());

	auto merge_level = left->_get_level();
	
	merge_node->_set_level(merge_level);

	for (int i = 0; i < parentp->_num_child(); i++) {
		if (parentp->_childAt(i) == right) {
			merge_node->_insert_key(parentp->_keysAt(i - 1).first, parentp->_keysAt(i - 1).second);
			parentp->_remove_key(parentp->_keysAt(i - 1).first);
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

	left.reset();

	right.reset();

	return parentp;
}

bool btree::_test_merge(shared_ptr<btnode>& node, shared_ptr<btnode>& pnode, shared_ptr<btnode>& rnode) {

	cout << __func__ << endl;

	// node does not violate b-tree property
	if (node->_num_keys() >= _max_children/2)
		return false;  

	auto parentp = node->_parentp();

	cout << "Parent : " << parentp->_num_child() << "<" << parentp->_min << "," << parentp->_max << ">" << endl;

	for (int i = 0; i < parentp->_num_child(); i++) {
		if (parentp->_childAt(i)->_min > parentp->_max) {
			rnode = parentp->_childAt(i);
			break;
		}
	}
		
	for (int i = parentp->_num_child() - 1; i >= 0; i--) {
		cout << "Child : " << "<" << parentp->_childAt(i)->_min << "," << parentp->_childAt(i)->_max << ">" << endl;
		if (parentp->_childAt(i)->_max < parentp->_min) {
			pnode = parentp->_childAt(i);
			break;
		}
	}

	// Test Left Sibling
	if (pnode && (pnode->_num_keys() > _max_children/2))
		return false;

	// Test Right Sibling
	if (rnode && (rnode->_num_keys() > _max_children/2))
		return false;

	cout << "left node key  : < " << pnode->_min << "," << pnode->_max << ">" << endl;
	cout << "right node key : < " << rnode->_min << "," << rnode->_max << ">" << endl;

	return true;
}
/*@
 *  This assumes both the left and right child are leaf nodes
 *
 */
void btree::_do_right_rotation(shared_ptr<btnode>& left, shared_ptr<btnode>& right, shared_ptr<btnode>& node) {

	TRACE_FUNC(__func__);

	assert (right && (right->_num_keys() < _max_children/2));

	assert (node && (node->_num_keys() >= _max_children/2));

	assert (left && (left->_num_keys() >= _max_children/2));

	element_t mid_key;

	for (int i = 0; i < node->_num_keys(); i++) {
		if (node->_keysAt(i).first < right->_min)
			mid_key = node->_keysAt(i);
		else
			break;
	}

	node->_remove_key(mid_key.first);

	right->_insert_key(mid_key.first, mid_key.second);

	// in-order predecessor in left child to parent node for RL-rotation
	 
	int max_index = left->_find_key(left->_max);

	element_t lkey = left->_keysAt(max_index);

	left->_remove_key(lkey.first);

	node->_insert_key(lkey.first, lkey.second);

	auto sibling = left->_childAt(max_index + 1);

	right->_insert_child(sibling);

	left->_remove_child(sibling);
}

void btree::_do_left_rotation(shared_ptr<btnode>& left, shared_ptr<btnode>& right, shared_ptr<btnode>& node) {

	TRACE_FUNC(__func__);

	assert(left && (left->_num_keys() < _max_children/2));

	assert(right && (right->_num_keys() >= _max_children/2));

	assert(node && (node->_num_keys() >= _max_children/2));

	element_t mid_key;

	for (int i = 0; i < node->_num_keys(); i++) {
		if (node->_keysAt(i).first > left->_max)
			mid_key = node->_keysAt(i);
		else
			break;
	}

	node->_remove_key(mid_key.first);

	left->_insert_key(mid_key.first, mid_key.second);

	// Pick From Edges
	
	int min_index = right->_find_key(right->_min);

	element_t rkey = right->_keysAt(min_index);

	right->_remove_key(rkey.first);

	node->_insert_key(rkey.first, rkey.second);

	assert(min_index == 0);

	auto sibling = left->_childAt(min_index);

	left->_insert_child(sibling);

	right->_remove_child(sibling);
}


shared_ptr<btnode> btree::_do_lookup(const bkey_t key, shared_ptr<btnode> node) {

	TRACE_FUNC(__func__);

	shared_ptr<btnode> rnode;

	assert (node);

	cout << "lookup node <" << node->_min << "," << node->_max << ">" << endl;

	for (int i = 0; i < node->_num_keys(); i++)
		if (key == node->_keysAt(i).first)
			return node;

	for (int i = 0; i < node->_num_child(); i++) {
		rnode = node->_childAt(i);
		cout << "lookup node <" << rnode->_min << "," << rnode->_max << ">" << endl;
		//if ((key >= rnode->_min) && (key <= rnode->_max))
		if (key <= rnode->_min)
			return _do_lookup(key, rnode);
	}

	if (node->_num_child())
		return _do_lookup(key, rnode);

	return nullptr;
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
	return (node->_num_child() >= _max_children/2) ? true : false;
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

	auto curr = _lookup(key);
	if (!curr) {
		cout << "key " << key << " not found " << endl;
		return;
	}

	auto pprev = _inorder_predecessor(key);

	assert(pprev->_num_child() == 0);

	cout << "key : " << key << "inorder predecessor : " << pprev->_max << endl;

	if (curr != pprev) {

		// right-most key
		
		int rpos = pprev->_num_keys() - 1;

		auto val = pprev->_keysAt(rpos);

		assert(val.first == pprev->_max);

		//replace
	
		curr->_remove_key(key);

		curr->_insert_key(val.first, val.second);

		pprev->_remove_key(val.first);
	
	} else 
		curr->_remove_key(key);

	auto node = pprev;

	cout << "pprev : " << pprev->_num_keys() << "<" << pprev->_min << "," << pprev->_max << ">" << endl;
	cout << "curr  : " << curr->_num_keys()  << "<" << curr->_min  << "," << curr->_max  << ">" << endl;

	while ((node->_isLeaf() && !_test_valid_leaf(node)) || 
	      (!node->_isLeaf() && !(_test_valid_non_leaf(node)))) {

		shared_ptr<btnode> parentp = node->_parentp();

		shared_ptr<btnode> pnode = nullptr;

		shared_ptr<btnode> nnode = nullptr; 

		if (_test_merge(node, pnode, nnode))
			node =_do_merge(parentp, pnode, nnode);
		else
		if (pnode->_num_keys() >= _max_children/2)
			_do_right_rotation(pnode, nnode, node);
		else
		if (nnode->_num_keys() >= _max_children/2)
			_do_left_rotation(pnode, nnode, node);
	}

	// In case re-balancing propagated to Root
	if ((node == _rootp) && (0 == node->_num_keys())) {
		_rootp = node->_childAt(0);
		 node.reset();
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
		 	cout << "btkey : " << node->_keysAt(i).first << endl;
	} else {
		for (auto i = 0; i < num_c; i++) {
			_do_print(node->_childAt(i));
		 	if (i < num_k)
		 		cout << "btkey : " << node->_keysAt(i).first << endl;
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
				cout << p->_keysAt(i).first << ",";
			for (int i = 0; i < p->_num_child(); i++)
				q2.push(p->_childAt(i));

			n1++;
		} 

		cout << endl << "Level :" << p->_get_level() << " Nodes : " << n1 << endl;

		while (q2.size()) {
			p = q2.front();

			q2.pop();
			
			for (int i = 0; i < p->_num_keys(); i++)
				cout << p->_keysAt(i).first << ",";
			for (int i = 0; i < p->_num_child(); i++)
				q1.push(p->_childAt(i));
		
			n2++;
		}

		cout << endl << "Level :" << p->_get_level() << " Nodes : " << n2 << endl;
	}
}

void btree::_print(void) const {

//	_do_print(_rootp);
	_do_level_traversal(_rootp);
}

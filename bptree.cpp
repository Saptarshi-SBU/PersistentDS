/*----------------------------------------------------------------------
 * B+-Tree Properties
 * -Every node has at most m children.
 * -Every non-leaf, non-root node has at least floor(d/2) children
 * -The root has atleat two children
 * -Every leaf contains at least floor(d/2) children
 * -All leaves appear in the same level
 *  --------------------------------------------------------------------*/

#include <iostream>
#include <cassert>
#include <queue>
#include <algorithm>
#include "bptree.h"
#include "trace.h"
#include "boost_logger.h"

bptree::bptree(int max) : _max_children(max) {

    if (max < 3)
        throw "min branching factor expected is 3";
    else {
        _k = max/2;

        // BUG FIX : First Node to be allocated in a B+ Tree is a leaf node
        _rootp = blkptr_t (new bptnode_leaf(blkptr_internal_t(nullptr), 0));
        _headp = _tailp = LEAF(_rootp);

        _total_merges = _total_splits = 0;
        _total_nodes = 1;
    }
}

bptree::~bptree() {
    if (_rootp)
        _tree_destroy(_rootp);
}

// We do post-order traversal to de-allocate all nodes of
// the tree. Note  we need to drop the reference that each
// node has to its parent and vice-versa
void
bptree::_tree_destroy(blkptr_t node) {

    for (auto i = 0; i < node->_num_child(); i++) {
         node->_childAt(i).reset();
        _tree_destroy(node->_childAt(i));
    }

    node->reset_parentp();
    node.reset();
    _total_nodes--;
}

// To retrive the block contents, we need to access the leaf.
// In each block, we search which blockptr has a range which
// includes our key. Note we can return node type root or leaf.
blkptr_leaf_t
bptree::_tree_get_leaf_node(const bkey_t key, blkptr_t& node) {

    blkptr_t next;

    if (!node) {
        BOOST_LOG_TRIVIAL(error) << "leaf node not found!";
        return blkptr_leaf_t();
    }

    if (!(node->_num_child())) {
        BOOST_LOG_TRIVIAL(debug) << "leaf node found";
        return LEAF(node);
    }

    for (int i = 0; i < node->_num_child(); i++) {
        next = node->_childAt(i);
        BOOST_LOG_TRIVIAL(debug) << "max cached: " << next->_max_cached;
        if (key > next->_max_cached)
            continue;
        break;
    }

    return _tree_get_leaf_node(key, next);
}

// To find internal node, which has references to a key in the leaf
// This is needed to remove the reference and update it with the
// next key in sucession in inorder fashion.
//
// The caller must ensure that the key is not in the root prior invoking
// this function
blkptr_internal_t
bptree::_tree_get_internal_node(const bkey_t key, blkptr_t& node) {

    if (!node) {
        BOOST_LOG_TRIVIAL(error) << "internal node not found!";
        return blkptr_internal_t();
    }

    if (node->find_key(key) > 0) {
        BOOST_LOG_TRIVIAL(debug) << "internal node found";
        return INTERNAL(node);
    }

    for (int i = 0; i < node->_num_child(); i++) {
        auto node_in_range = node->_childAt(i);
        if (key > node_in_range->_max_cached)
            continue;
        return _tree_get_internal_node(key, node_in_range);
    }

    return blkptr_internal_t();
}

// B+-Tree Insert Algorithm
void
bptree::_tree_insert(const index_t key, mapping_t val, blkptr_t& node) {

    if (!node)
        return;

    auto leaf = _tree_get_leaf_node(key, node);

    assert (leaf && (leaf->_type == Leaf));
    leaf->insert_record(key, val);

    if (leaf->_num_keys() >= _max_children)
        _node_split(dynamic_pointer_cast<bptnode_raw>(leaf));
}

// B+-Tree Delete Algorithm
void
bptree::_tree_delete(const bkey_t key) {
    if (!_rootp)
        return;

    auto curr = _tree_lookup(key, _rootp);
    if (!curr) {
        BOOST_LOG_TRIVIAL(error) << "key not found " << key;
        return;
    }

    auto leaf = _tree_get_leaf_node(key, _rootp);
    assert(leaf && leaf->_type == Leaf);

    leaf->remove_key(key);
    BOOST_LOG_TRIVIAL(info) << "key removed from leaf " << key;

    if (leaf == LEAF(_rootp)) {
        if (!_rootp->_num_keys())
            _rootp.reset();
        return;
    }

    // Drop any associated references if any and update with new one
    auto internal = _tree_get_internal_node(key, _rootp);
    // We may or may not have internal node with the key
    if (internal) {
        internal->remove_key(key);
        internal->insert_key(leaf->_min_cached);
        BOOST_LOG_TRIVIAL(info) << "key removed from parent " << key;
    }

    auto node = _tree_rebalance(leaf);

    // Rebalance Propagated to Root
    if (node == _rootp) {
        // Root is Empty
        if (!node->_num_keys()) {
            _rootp.reset();
            // Pending Child becomes the new root
            if (node->_num_child())
               _rootp = node->_childAt(0);
        }
    }
}

//B+-Tree LookUp Algorithm
blkptr_t
bptree::_tree_lookup(const bkey_t key, blkptr_t node) {

    assert(node);

    BOOST_LOG_TRIVIAL(debug)
        << FMTRANGE(node->_min_cached, node->_max_cached);

    if ((!(node->_num_child())) && (node->find_key(key) >= 0)) {
        return node;
    } else if (!(node->_num_child())) {
        BOOST_LOG_TRIVIAL(error) << "key not found " << key;
        return blkptr_t(nullptr);
    } else {
        int i;
        for (i = 0; i < node->_num_keys(); i++)
        if (key < node->_keysAt(i))
                return _tree_lookup(key, node->_childAt(i));
        return _tree_lookup(key, node->_childAt(i));
    }
}

// Tree Rebalancing Algorithm
// Drive rebalancing operations if needed on the tree.
//
blkptr_t
bptree::_tree_rebalance(blkptr_t curr) {

     assert (curr);

     int nr_keys = curr->_num_keys();

     // Check if rebalance required
     if (_rootp == curr)
         return curr;
     else if (check_range(nr_keys, _k, _max_children - 1))
         return curr;
     else {

         BOOST_LOG_TRIVIAL(debug) << "rebalance started";

         // Find the members participating in the rebalance
         // parent and the siblings
         auto prev_sib = blkptr_t(nullptr);

         auto next_sib = blkptr_t(nullptr);

         auto parentp = curr->_parentp();

         // rebalance not required for root
         assert(parentp);

         int iter = 0;

         while (iter < INTERNAL(parentp)->_num_child()) {
             // We locate the child node
             // Now get nearby candidate siblings
         if (INTERNAL(parentp)->_childAt(iter) == curr) {
                 if (iter > 0)
                     prev_sib = parentp->_childAt(iter - 1);
                 if (iter < (INTERNAL(parentp)->_num_child() - 1))
                     next_sib = parentp->_childAt(iter + 1);
                 break;
             }
             iter++;
         }

         // Choose the type of rebalance needed
         // +)Steal
         // +)Merge

         // Keys should be greater than the minimum to able to steal
         if (prev_sib && (prev_sib->_num_keys() > _k)) {
             // Steal from left Child
             _node_steal_from_lsibling(curr, parentp, prev_sib);

         } else if (next_sib && (next_sib->_num_keys() > _k)) {
            // Steal from right child
            _node_steal_from_rsibling(curr, parentp, next_sib);

         } else {
            // insufficient sibling keys. Merge is needed
            // FIX : check for null
            if (next_sib)
                parentp = _node_merge(INTERNAL(parentp), curr, next_sib);
            else if (prev_sib)
                parentp = _node_merge(INTERNAL(parentp), prev_sib, curr);
            else
                assert(0);

            // merge may trigger next round of rebalance
            parentp = _tree_rebalance(parentp);
         }

         return parentp;
     }
}

//B+Tree Traversal
void
bptree::_tree_print(const blkptr_t& node) const {

    if (!node)
        return;

    if (!node->_num_child()) {
        for (auto i = 0; i < node->_num_keys(); i++)
            BOOST_LOG_TRIVIAL(info) << "inorder btkey : " <<  node->_keysAt(i);
        return;
    }

    for (auto i = 0; i < node->_num_child(); i++)
            _tree_print(node->_childAt(i));
    return;
}

void
bptree::_tree_level_traversal(const blkptr_t& node) const {

    if (!node)
        return;

    queue<shared_ptr<bptnode_raw>> q1, q2;
    shared_ptr<bptnode_raw> p;
    int n1, n2;
    bool bFlag = false;

    q1.push(node);

    while (q1.size() || q2.size()) {

        n1 = n2 = 0;

        while (q1.size()) {
            p = q1.front();
            q1.pop();
            std::string str(" ");
            for (int i = 0; i < p->_num_keys(); i++) {
                str.append(to_string(p->_keysAt(i)));
                                str.append(", ");
            }
            for (int i = 0; i < p->_num_child(); i++)
                q2.push(p->_childAt(i));

            str.append("<");
            str.append(to_string(p->_num_child()));
            str.append(">");
            BOOST_LOG_TRIVIAL(info)  << str;
            n1++;
            bFlag = true;
        }

        if (bFlag) {
            BOOST_LOG_TRIVIAL(info)
                 << FMTLEVEL(p->_get_level()) << "Nodes : " << n1;
            bFlag = false;
        }

        while (q2.size()) {
            p = q2.front();
            q2.pop();
            std::string str(" ");

            for (int i = 0; i < p->_num_keys(); i++) {
                str.append(to_string(p->_keysAt(i)));
                                str.append(", ");
            }
            for (int i = 0; i < p->_num_child(); i++)
                q1.push(p->_childAt(i));

            str.append("<");
            str.append(to_string(p->_num_child()));
            str.append(">");
            BOOST_LOG_TRIVIAL(info)  << str;
            n2++;
            bFlag = true;
        }

        if (bFlag) {
            BOOST_LOG_TRIVIAL(info)
                 << FMTLEVEL(p->_get_level()) << "Nodes : " << n2;
            bFlag = false;
        }
    }
}

void
bptree::_tree_leaf_walk(blkptr_leaf_t node, dir_t dir) const {

    assert(node);

    int count = 0;

    switch(dir) {
    case REVERSE:
        for (;node && (++count); node=node->_prev)
            node->print();
        break;

    case FORWARD:
        for (;node && (++count); node=node->_next)
            node->print();
        break;
    }

    BOOST_LOG_TRIVIAL(info) << "total leaf nodes" << count;
}

// Node Split Steps. We invoke this when a node fill reaches
// its capacity. Note we have a extra slot for the keys before
// we start detecting a constraint violation.
void
bptree::_node_split(blkptr_t node) {

    assert(node && (node->_num_keys() >= _max_children));

    blkptr_t parentp, lchildp, rchildp;

    int split_index = node->_separator();
    int level = node->_get_level();

    // Parent needs to be updated with the siblings once created
    parentp = node->_parentp();
    if (!parentp) {
        parentp = blkptr_internal_t
            (new bptnode_internal(blkptr_internal_t(nullptr), level - 1));
        _total_nodes++;
    } else
        INTERNAL(parentp)->remove_child(node);

    // Split the node and create the siblings
    // B+-Tree needs separate treatment for leaf and internal nodes
    // on split unlike in a BTree.
    switch (node->_type) {
    case Leaf: {

        BOOST_LOG_TRIVIAL(debug) << "leaf : "
             << FMTRANGE(node->_min_cached, node->_max_cached);

        lchildp = blkptr_leaf_t
            (new bptnode_leaf(INTERNAL(parentp), level));
        rchildp = blkptr_leaf_t
            (new bptnode_leaf(INTERNAL(parentp), level));

        // Re-insert Record to the pair of siblings formed
        for (int i = 0; i < split_index; i++)
            LEAF(lchildp)->insert_record(node->_keysAt(i),
                    LEAF(node)->_kv[node->_keysAt(i)]);

        for (int i = split_index; i < node->_num_keys(); i++)
            LEAF(rchildp)->insert_record(node->_keysAt(i),
                    LEAF(node)->_kv[node->_keysAt(i)]);

        // Duplicate the key in the Parent
        parentp->insert_key(rchildp->_min_cached);

        // Update the leaf nodes link chain
        LEAF(lchildp)->update_chain(LEAF(node)->_prev, LEAF(rchildp));
        LEAF(rchildp)->update_chain(LEAF(lchildp), LEAF(node)->_next);

        if (_headp == LEAF(node)) {
            BOOST_LOG_TRIVIAL(debug) << "head updated";
            _headp = LEAF(lchildp);
        }
        if (_tailp == LEAF(node)) {
            BOOST_LOG_TRIVIAL(debug) << "tail updated";
            _tailp = LEAF(rchildp);
        }

        //_tree_leaf_walk(LEAF(rchildp), REVERSE);
        _total_nodes+=2;
        break;
    }

    case Internal:
        //
        // Fall through
        //
    case Root: {

        BOOST_LOG_TRIVIAL(debug) << "internal : "
             << FMTRANGE(node->_min_cached, node->_max_cached);

        lchildp = blkptr_internal_t
            (new bptnode_internal(INTERNAL(parentp), level));
        rchildp = blkptr_internal_t
            (new bptnode_internal(INTERNAL(parentp), level));

        // Re-insert Keys to the pair of siblings formed
        for (int i = 0; i < split_index; i++)
            lchildp->insert_key(node->_keysAt(i));

        for (int i = split_index + 1; i < node->_num_keys(); i++)
            rchildp->insert_key(node->_keysAt(i));

        // Re-insert Child to the pair of siblings formed
        for (int i = 0; i <= split_index; i++) {
            INTERNAL(lchildp)->insert_child(node->_childAt(i));
            node->_childAt(i)->set_parentp(lchildp);
        }

        for (int i = split_index + 1; i < node->_num_child(); i++) {
            INTERNAL(rchildp)->insert_child(node->_childAt(i));
            node->_childAt(i)->set_parentp(rchildp);
        }

        // Keep the middle-value in the parent only (unlike leaf)
        parentp->insert_key(node->_keysAt(split_index));
        _total_nodes+=2;
        break;
    }

    default:
        assert(0);
    }

    // Update old and new references

    INTERNAL(parentp)->insert_child(lchildp);
    INTERNAL(parentp)->insert_child(rchildp);

    // If node was the root node
    if (_rootp == node) {
        _rootp = parentp;
        BOOST_LOG_TRIVIAL(debug) << "new root";
    }

    //FIX : Compare shared_ptr prior reset
    node->reset_parentp();
    node.reset();

    _total_nodes--;
    _total_splits++;

    // In case, siblings addition to parent violated constraint
    if (parentp->_num_keys() >= _max_children)
        _node_split(parentp);

}

//This is invoked when the MIN constraint is violated.
//The participating nodes are the pair of siblings and the parent
//This is the last resort in re-balancing process when stealing is
//not possible
//
blkptr_t
bptree::_node_merge(blkptr_internal_t parentp,
                    blkptr_t lchildp,
                    blkptr_t rchildp) {

    //sanity rule check prior merge
    assert(parentp && (parentp->_num_child() >= 2));

    // FIX : check constraint with the OR logic
    assert((lchildp && (lchildp->_num_keys() < _k)) ||
           (rchildp && (rchildp->_num_keys() < _k)));

    blkptr_t merge_node;

    node_t merge_type = lchildp->_type;

    int merge_level = lchildp->_get_level();

    // Allocate Merge Node
    if (Leaf == merge_type)
        merge_node = blkptr_leaf_t
            (new bptnode_leaf(parentp, merge_level));
    else
        merge_node = blkptr_internal_t
            (new bptnode_internal(parentp, merge_level));

    switch (merge_node->_type) {
    case Leaf: {

        BOOST_LOG_TRIVIAL(debug) << "merging nodes";
#if DEBUG
        parentp->print();
        LEAF(lchildp)->print();
        LEAF(rchildp)->print();
#endif

        // Merge Sibling Record
        for (int i = 0; i < lchildp->_num_keys(); i++)
            LEAF(merge_node)->insert_record(lchildp->_keysAt(i),
                    LEAF(lchildp)->_kv[lchildp->_keysAt(i)]);

        for (int i = 0; i < rchildp->_num_keys(); i++)
            LEAF(merge_node)->insert_record(rchildp->_keysAt(i),
                    LEAF(rchildp)->_kv[rchildp->_keysAt(i)]);

        // Repair the Links
        LEAF(merge_node)->update_chain(LEAF(lchildp)->_prev,
                LEAF(rchildp)->_next);

        //LEAF(merge_node)->print();

        // Remove the duplicate key entry in the parent
        parentp->remove_key(rchildp->_min_cached);

        BOOST_LOG_TRIVIAL(debug) << "parent entry cleared(leaf) "
            << rchildp->_min_cached;
        break;
    }

    case Internal:
        //
        // Fall through
        //
    case Root: {

        BOOST_LOG_TRIVIAL(debug) << "merging nodes";
#if DEBUG
        parentp->print();
        INTERNAL(lchildp)->print();
        INTERNAL(rchildp)->print();
#endif

        // Merge Sibling Keys
        for (int i = 0; i < lchildp->_num_keys(); i++)
            INTERNAL(merge_node)->insert_key(lchildp->_keysAt(i));

        for (int i = 0; i < rchildp->_num_keys(); i++)
            INTERNAL(merge_node)->insert_key(rchildp->_keysAt(i));

        // Merge Sibling branch entries
        for (int i = 0; i < lchildp->_num_child(); i++) {
            INTERNAL(merge_node)->insert_child(lchildp->_childAt(i));
            lchildp->_childAt(i)->set_parentp(merge_node);
        }

        for (int i = 0; i < rchildp->_num_child(); i++) {
            INTERNAL(merge_node)->insert_child(rchildp->_childAt(i));
            rchildp->_childAt(i)->set_parentp(merge_node);
        }

        // Remove the split key entry in the parent and
        // add to the Merge Node. Note lower bound implementation
        // is different than provided by STL implementation
        auto low = _lower_bound(parentp->_keys.begin(),
                parentp->_keys.end(), rchildp->_min_cached);

        BOOST_LOG_TRIVIAL(debug) << "lower bound key <" << rchildp->_min_cached
            << ":" << *low << ">";

        merge_node->insert_key(*low);

        //INTERNAL(merge_node)->print();

        parentp->remove_key(*low);

        BOOST_LOG_TRIVIAL(debug) << "parent entry cleared(internal "
                << rchildp->_min_cached;
        break;
    }

    default:
        assert(0);
    }

    // Update old and new references
    parentp->remove_child(lchildp);
    parentp->remove_child(rchildp);
    parentp->insert_child(merge_node);

    //FIX: Ensure we remove references to the existing parent
    lchildp->reset_parentp();
    rchildp->reset_parentp();

    lchildp.reset();
    rchildp.reset();

    merge_node->set_parentp(parentp);
    _total_merges++;

    return parentp;
}

//Stealing from left
void
bptree::_node_steal_from_lsibling(blkptr_t& curr,
                                  blkptr_t& parentp,
                                  blkptr_t& left) {

     // Verify we meet the stealing Constraints
     //
    assert (curr && (curr->_num_keys() < _k));

    assert (left && (left->_num_keys() > _k));

    index_t steal_key;

    switch(curr->_type) {
    case Leaf: {

        steal_key = left->_max_cached;

        parentp->remove_key(curr->_min_cached);

        LEAF(curr)->insert_record(steal_key, LEAF(left)->_kv[steal_key]);
        parentp->insert_key(curr->_min_cached);

        LEAF(left)->remove_key(steal_key);

        BOOST_LOG_TRIVIAL(debug) << "stolen key from left sibling" << steal_key;
        break;
    }

    case Root:
    case Internal: {

        // NB: for internal nodes child keys are excluded from parent
        // Note lower bound implementation is different than provided
        // by STL implementation
        auto upper = _upper_bound(parentp->_keys.begin(),
                parentp->_keys.end(), (const index_t)left->_max_cached);

        int pos = left->_num_child();
        auto sib = left->_childAt(pos - 1);

        steal_key = *upper;

        parentp->remove_key(steal_key);
        parentp->insert_key(left->_max_cached);

        curr->insert_key(steal_key);
        INTERNAL(curr)->insert_child(sib);

        left->remove_key(left->_max_cached);
        INTERNAL(left)->remove_child(sib);

        BOOST_LOG_TRIVIAL(debug) << "stolen key from left sibling" << steal_key;
        break;
    }

    default:
        assert(0);
    }
}

//Stealing from right
void
bptree::_node_steal_from_rsibling(blkptr_t& curr,
                                  blkptr_t& parentp,
                                  blkptr_t& right) {

    //Sanity
    assert(curr && (curr->_num_keys() < _k));

    assert(right && (right->_num_keys() > _k));

    //assert(parentp && (parentp->_num_keys() > _k));

    index_t steal_key;

    switch(curr->_type) {
    case Leaf:

        steal_key = right->_min_cached;

        LEAF(curr)->insert_record(steal_key, LEAF(right)->_kv[steal_key]);
        right->remove_key(steal_key);

        parentp->remove_key(steal_key);
        parentp->insert_key(right->_min_cached);

        BOOST_LOG_TRIVIAL(debug) << "stolen key from right sibling" << steal_key;
        break;

    case Root:
    case Internal: {
        // NB: for internal nodes child keys are excluded from parent
        // Note lower bound implementation is different than provided
        // by STL implementation
        auto low = _lower_bound(parentp->_keys.begin(),
                parentp->_keys.end(), right->_min_cached);

        steal_key = *low;
        auto sib = right->_childAt(0);

        curr->insert_key(steal_key);
        INTERNAL(curr)->insert_child(sib);

        parentp->remove_key(steal_key);
        parentp->insert_key(right->_min_cached);

        right->remove_key(right->_min_cached);
        INTERNAL(right)->remove_child(sib);

        BOOST_LOG_TRIVIAL(debug) << "stolen key from right sibling" << steal_key;
        break;
    }

    default:
        assert(0);
    }
}

//B+-Tree:API
void bptree::insert(const index_t key, const mapping_t val) {
    BOOST_LOG_TRIVIAL(info) << " insert key : " << key;

    _tree_insert(key, val, _rootp);
}

//LookUp API
blkptr_t bptree::lookup(const index_t key) {
    BOOST_LOG_TRIVIAL(info) << "lookup key : " << key;

    return _tree_lookup(key, _rootp);
}

//Remove API
void bptree::remove(const index_t key) {
    BOOST_LOG_TRIVIAL(info) << "remove key : " << key;

    _tree_delete(key);
}

//Print API
void bptree::print(void) const {

     if (!_rootp)
         return;

     BOOST_LOG_TRIVIAL(info) << " ###Inorder Traversal###";
     _tree_print(_rootp);

     BOOST_LOG_TRIVIAL(info) << " ###Level order Traversal###";
     _tree_level_traversal(_rootp);

     BOOST_LOG_TRIVIAL(info) << " ###Linked List Traversal (Forward)###";
     _tree_leaf_walk(_headp, FORWARD);

     BOOST_LOG_TRIVIAL(info) << " ###Linked List Traversal (Reverse)###";
     _tree_leaf_walk(_tailp, REVERSE);
}

//Print API
void bptree::stats(void) const {
    BOOST_LOG_TRIVIAL(info) << " #####Statistics######";
    BOOST_LOG_TRIVIAL(info) << "total nodes" << _total_nodes;
    BOOST_LOG_TRIVIAL(info) << "total splits" << _total_splits;
    BOOST_LOG_TRIVIAL(info) << "total merges" << _total_merges;
}

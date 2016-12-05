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

#if DEBUG
static bool debug = true;
#else
static bool debug = false;
#endif

bptree::bptree(int max) : _max_children(max) {

    if (max < 3)
        throw "min branching factor expected is 3";
    else {
        _rootp = blkptr_t (new bptnode_internal(blkptr_internal_t(nullptr), max));
        // Min Capacity Constraint
        _k = max/2;
    }
}

bptree::~bptree() {
    if (_rootp)
        _tree_destroy(_rootp);
}

// We do post-order traversal to de-allocate all nodes of
// the tree. Note  we need to drop the reference that each
// node has to its parent and vice-versa
void bptree::_tree_destroy(blkptr_t node) {

    for (auto i = 0; i < node->_num_child(); i++) {
         node->_childAt(i).reset();
        _tree_destroy(node->_childAt(i));
    }

    node->reset_parentp();
    node.reset();
}

// To retrive the block contents, we need to access the leaf.
// In each block, we search which blockptr has a range which
// includes our key. Note we can return node type root or leaf.
blkptr_leaf_t bptree::_tree_get_leaf_node(const bkey_t key, blkptr_t& node) {

    if (!node)
        return blkptr_leaf_t();

    if (!(node->_num_child()))
        return LEAF(node);

    for (int i = 0; i < node->_num_child(); i++) {
        auto node_in_range = node->_childAt(i);
        if (key > node_in_range->_max_cached)
            continue;
        return _tree_get_leaf_node(key, node_in_range);
    }
}

// To find internal node, which has references to a key in the leaf
// This is needed to remove the reference and update it with the
// next key in sucession in inorder fashion.
blkptr_internal_t bptree::_tree_get_internal_node(const bkey_t key, blkptr_t& node) {

    if (!node)
        return blkptr_internal_t();

    // We should never reach the leaf
    assert(node->_type == Leaf);

    if (node->find_key(key) > 0)
        return INTERNAL(node);

    for (int i = 0; i < node->_num_child(); i++) {
        auto node_in_range = node->_childAt(i);
        if (key > node_in_range->_max_cached)
            continue;
        return _tree_get_internal_node(key, node_in_range);
    }
}

// Node Split Steps. We invoke this when a node fill reaches
// its capacity. Note we have a extra slot for the keys before
// we start detecting a constraint violation.
void bptree::_node_split(blkptr_t node) {

    assert(node && (node->_num_keys() >= _max_children));

    blkptr_t parentp, lchildp, rchildp;

    int split_index = node->_separator();
    int level = node->_get_level();

    // Parent needs to be updated with the siblings once created
    parentp = node->_parentp();
    if (!parentp)
        parentp = blkptr_internal_t (new bptnode_internal(blkptr_internal_t(nullptr), level - 1));

    // Split the node and create the siblings
    // B+-Tree needs separate treatment for leaf and internal nodes
    // on split unlike in a BTree.
    switch (node->_type) {
    case Leaf: {

        lchildp = blkptr_leaf_t (new bptnode_leaf(INTERNAL(parentp), level));
        rchildp = blkptr_leaf_t (new bptnode_leaf(INTERNAL(parentp), level));

        // Re-insert Record to the pair of siblings formed
        for (int i = 0; i < split_index; i++)
            LEAF(lchildp)->insert_record(node->_keysAt(i), LEAF(node)->_kv[node->_keysAt(i)]);

        for (int i = split_index; i < node->_num_keys(); i++)
            LEAF(rchildp)->insert_record(node->_keysAt(i), LEAF(node)->_kv[node->_keysAt(i)]);

        // Duplicate the key in the Parent
        parentp->insert_key(rchildp->_min_cached);

        // Update the leaf nodes link chain
        LEAF(lchildp)->update_chain(LEAF(node)->_prev, LEAF(rchildp));
        LEAF(rchildp)->update_chain(LEAF(lchildp), LEAF(node)->_next);

        break;
    }

    case Internal:
        //
        // Fall through
        //
    case Root: {

        lchildp = blkptr_internal_t (new bptnode_internal(INTERNAL(parentp), level));
        rchildp = blkptr_internal_t (new bptnode_internal(INTERNAL(parentp), level));

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
        break;
    }

    default:
        assert(0);
    }

    // Update old and new references
    INTERNAL(parentp)->remove_child(node);

    INTERNAL(parentp)->insert_child(lchildp);
    INTERNAL(parentp)->insert_child(rchildp);

    node->reset_parentp();
    node.reset();

    // If node was the root node
    if (_rootp == node)
        _rootp = parentp;

    // In case, siblings addition to parent violated constraint
    if (parentp->_num_keys() >= _max_children)
        _node_split(parentp);

    trace_record(debug, __func__, "split node use count :", node.use_count());
    trace_record(debug, __func__, "split separator key :", split_index);
}

// B+-Tree insert operation algorithm
void bptree::_tree_insert(const index_t key, mapping_t val, blkptr_t& node) {

    if (!node)
        return;

    auto leaf = _tree_get_leaf_node(key, node);
    assert (leaf);

    leaf->insert_record(key, val);

    trace_record(debug, __func__, key);

    if (leaf->_num_keys() >= _max_children)
        _node_split(dynamic_pointer_cast<bptnode_raw>(leaf));
}

//B+-Tree:API
void bptree::insert(const index_t key, const mapping_t val) {
    _tree_insert(key, val, _rootp);
}

//This is invoked when the MIN constraint is violated.
//The participating nodes are the pair of siblings and the parent
//This is the last resort in re-balancing process when stealing is
//not possible
blkptr_t bptree::_node_merge(blkptr_internal_t parentp, blkptr_t lchildp, blkptr_t rchildp) {

    //sanity rule check prior merge
    assert(parentp && (parentp->_num_child() >= 2));

    assert(left && (lchildp->_num_keys() <= (_k - 1)));

    assert(right && (rchildp->_num_keys() <= (_k - 1)));

    blkptr_t merge_node;

    node_t merge_type = lchildp->_type;

    int merge_level = lchildp->_get_level();

    // Allocate Merge Node
    if (Internal == merge_type)
        merge_node = blkptr_leaf_t (new bptnode_leaf(parentp, merge_level));
    else
        merge_node = blkptr_internal_t (new bptnode_internal(parentp, merge_level));

    switch (merge_node->_type) {
    case Leaf: {

        // Merge Sibling Record
        for (int i = 0; i < lchildp->_num_keys(); i++)
            LEAF(merge_node)->insert_record(lchildp->_keysAt(i), LEAF(lchildp)->_kv[lchildp->_keysAt(i)]);

        for (int i = 0; i < rchildp->_num_keys(); i++)
            LEAF(merge_node)->insert_record(rchildp->_keysAt(i), LEAF(rchildp)->_kv[rchildp->_keysAt(i)]);

        // Repair the Links
        LEAF(merge_node)->update_chain(LEAF(lchildp)->_prev, LEAF(rchildp)->_next);

        // Remove the duplicate key entry in the parent
        parentp->remove_key(rchildp->_min_cached);

        trace_record(debug, __func__, " key removed from parent ", rchildp->_min_cached);
        break;
    }

    case Internal:
        //
        // Fall through
        //
    case Root: {

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

        // Remove the split key entry in the parent and add to the Merge Node
        auto low = lower_bound(parentp->_keys.begin(),
                parentp->_keys.end(), rchildp->_min_cached);
        merge_node->insert_key(*low);
        parentp->remove_key(*low);
        break;
    }

    default:
        assert(0);
    }

    // Update old and new references
    parentp->remove_child(lchildp);
    parentp->remove_child(rchildp);
    parentp->insert_child(merge_node);

    merge_node->set_parentp(parentp);

    trace_record(debug, __func__, "Node : ");

    lchildp.reset();
    rchildp.reset();
    return parentp;
}

void bptree::_node_steal_from_lsibling(blkptr_t& curr, blkptr_t& parentp, blkptr_t& left) {

     // Verify we meet the stealing Constraints
    assert (curr && (curr->_num_keys() < (_k - 1)));
    assert (left && (left->_num_keys() >= _k));

    switch(curr->_type) {
    case Leaf: {
        auto steal_key = left->_max_cached;
        LEAF(curr)->insert_record(steal_key, LEAF(left)->_kv[steal_key]);
        parentp->remove_key(curr->_min_cached);
        LEAF(left)->remove_key(steal_key);
        parentp->insert_key(steal_key);
        break;
    }

    case Root:
    case Internal: {

        auto low = lower_bound(parentp->_keys.begin(),
                parentp->_keys.end(), (const index_t)left->_max_cached);

        parentp->remove_key(*low);
        curr->insert_key(*low);
        trace_record(debug, __func__ , " mid key : ", *low);

        // in-order predecessor in left child
        auto high = lower_bound(left->_keys.begin(),
                left->_keys.end(), left->_max_cached);

        auto pos = high - left->_keys.begin();

        auto sibling = left->_childAt(pos + 1);

        left->remove_key(*high);
        parentp->insert_key(*high);

        INTERNAL(curr)->insert_child(sibling);
        INTERNAL(left)->remove_child(sibling);
        break;
    }

    default:
        assert(0);
    }
}

void bptree::_node_steal_from_rsibling(blkptr_t& curr, blkptr_t& parentp, blkptr_t& right) {

    assert(curr && (curr->_num_keys() < (_k - 1)));
    assert(right && (right->_num_keys() >= _k));

    // Commented bcoz parent can be root
    //assert(parentp && (parentp->_num_keys() >= _k));

    switch(curr->_type) {
    case Leaf: {
        auto steal_key = right->_min_cached;
        LEAF(curr)->insert_record(steal_key, LEAF(right)->_kv[steal_key]);
        right->remove_key(steal_key);
        parentp->remove_key(steal_key);
        parentp->insert_key(right->_min_cached);
        break;
    }

    case Root:
    case Internal: {

        auto low = lower_bound(parentp->_keys.begin(),
                parentp->_keys.end(), right->_min_cached);

        curr->insert_key(*low);
        parentp->remove_key(*low);

        trace_record(debug, __func__ , " mid key : ", *low);

        // in-order successor to parent node for RL-rotation
        auto high = lower_bound(right->_keys.begin(),
                right->_keys.end(), right->_min_cached);

        auto pos = high - right->_keys.begin();

        auto sibling = right->_childAt(pos);

        right->remove_key(*high);
        parentp->insert_key(*high);

        INTERNAL(curr)->insert_child(sibling);
        INTERNAL(right)->remove_child(sibling);
        break;
    }

    default:
        assert(0);
    }
}

// This is the member for checking any constraint violation and driving any
// rebalancing operations if needed on the tree.
blkptr_t bptree::_tree_rebalance(blkptr_t curr) {

     assert (curr);

     int nr_keys = curr->_num_keys();

     auto lambda = [] (int nr_key, int min, int max) {
         return ((nr_key >= min) && (nr_key <= max));
     };

     auto min = (curr->_type == Root) ? 0 : _k;

     auto max = _max_children - 1;

     if (lambda(curr->_num_keys(), min, max))
        return curr;

     trace_record(debug, __func__, "rebalance triggered ");

     // Find the members participating in the rebalance
     auto parentp = curr->_parentp();

     auto low = lower_bound(parentp->_keys.begin(),
                parentp->_keys.end(), curr->_min_cached);

     blkptr_t prev_sib, next_sib;

     prev_sib = parentp->_childAt(low - parentp->_keys.begin());
     if (prev_sib == curr)
         prev_sib = blkptr_t(nullptr);

     next_sib = parentp->_childAt(low - parentp->_keys.begin());
     if (next_sib == curr)
         next_sib = blkptr_t(nullptr);

    // Choose the type of rebalance needed
     if (prev_sib && (prev_sib->_num_keys() >= _k)) {
         // Steal from left Child
        _node_steal_from_lsibling(curr, parentp, prev_sib);
     } else if (next_sib && (next_sib->_num_keys() >= _k)) {
         // Steal from right child
        _node_steal_from_rsibling(curr, parentp, next_sib);
     } else {
         // insufficient sibling keys. Merge is needed
         if (prev_sib == curr)
              parentp = _node_merge(INTERNAL(parentp), curr, next_sib);
         else
              parentp = _node_merge(INTERNAL(parentp), prev_sib, curr);

         // merge may trigger next round of rebalance
         parentp = _tree_rebalance(parentp);
     }

     return parentp;
}

// B+-Tree Key Deletion Algorithm
void bptree::_tree_delete(const bkey_t key) {

    trace_record(debug, __func__, "key ", key);

    if (!_rootp)
        return;

    auto curr = _tree_lookup(key, _rootp);
    if (!curr) {
        trace_record(debug, __func__, "ERR btkey ", key, " not found ");
        return;
    }

    auto leaf = _tree_get_leaf_node(key, _rootp);
    assert(leaf && leaf->_type == Leaf);
    leaf->remove_key(key);

   // Drop any associated references if any and update with new one
    auto internal = _tree_get_internal_node(key, _rootp);
    if (internal) {
        internal->remove_key(key);
        internal->insert_key(leaf->_min_cached);
    }

    auto node = _tree_rebalance(leaf);

    // Rebalance Propagated to Root
    if (node == _rootp) {
        // Root is Empty
        if (!node->_num_keys()) {
            // Pending Child becomes the new root
            if (node->_num_child())
               _rootp = node->_childAt(0);
            _rootp.reset();
        }
    }
}

//B+-Tree LookUp Algorithm
blkptr_t bptree::_tree_lookup(const bkey_t key, blkptr_t node) {

    assert (node);

    trace_record(debug, __func__, "lookup node <", node->_min_cached, ",", node->_max_cached, ">");

    if (!node->_num_child() && (node->find_key(key) > 0))
        return node;
    else if (!node->_num_child())
        return blkptr_t(nullptr);
    else {
        for (int i = 0; i < node->_num_child(); i++) {
            auto child = node->_childAt(i);
            if ((key >= child->_min_cached) && key <= child->_max_cached)
                return _tree_lookup(key, child);
        }
    }
}

//LookUp API
blkptr_t bptree :: lookup(const index_t key) {
    return _tree_lookup(key, _rootp);
}

//Remove API
void bptree::remove(const index_t key) {
    _tree_delete(key);
}

void bptree::_tree_print(const blkptr_t& node) const {

    trace_record(debug, __func__);

    if (!node)
        return;

    if (!node->_num_child()) {
        for (auto i = 0; i < node->_num_keys(); i++)
            trace_record(debug, __func__, " inorder btkey : ", node->_keysAt(i));
        return;
    }

    for (auto i = 0; i < node->_num_child(); i++)
            _tree_print(node->_childAt(i));
    return;
}

void bptree::_tree_level_traversal(const blkptr_t& node) const {

    if (!node)
        return;

    queue<shared_ptr<bptnode_raw>> q1, q2;
    shared_ptr<bptnode_raw> p;
    int n1, n2;

    q1.push(node);

    while (q1.size() || q2.size()) {

        n1 = n2 = 0;

        while (q1.size()) {
            p = q1.front();
            q1.pop();
            std::string str;
            for (int i = 0; i < p->_num_keys(); i++) {
                str.append(to_string(p->_keysAt(i)));
                                str.append(", ");
            }
            for (int i = 0; i < p->_num_child(); i++)
                q2.push(p->_childAt(i));

            str.append("<");
            str.append(to_string(p->_num_child()));
            str.append(">");
            trace_record(debug, __func__, str);
            n1++;
        }

        trace_record(debug, __func__, "Level : ", p->_get_level()," Nodes : ", n1);

        while (q2.size()) {
            p = q2.front();
            q2.pop();
            std::string str;

            for (int i = 0; i < p->_num_keys(); i++) {
                str.append(to_string(p->_keysAt(i)));
                                str.append(", ");
            }
            for (int i = 0; i < p->_num_child(); i++)
                q1.push(p->_childAt(i));

            str.append("<");
            str.append(to_string(p->_num_child()));
            str.append(">");
            trace_record(debug, __func__, str);
            n2++;
        }

        trace_record(debug, __func__, "Level : ", p->_get_level()," Nodes : ", n2);
    }
}

//Print API
void bptree::print(void) const {

     if (_rootp) {
         _tree_print(_rootp);
         _tree_level_traversal(_rootp);
     }
}



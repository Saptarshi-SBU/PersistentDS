#ifndef _BPTREE_H
#define _BPTREE_H

#include <memory>

#include "bptnode.h"

typedef index_t bkey_t;

typedef shared_ptr<bptnode_raw> blkptr_t;
typedef shared_ptr<bptnode_leaf> blkptr_leaf_t;
typedef shared_ptr<bptnode_internal> blkptr_internal_t;

// All Node Operations are O(1)
// All Tree Operatins are O(logN)

class bptree {

    private:

        // root node
        blkptr_t _rootp;

        // branch factor
        int _max_children;

        // constraint for number of keys
        int _k;

    protected:

        // Node Operations : Split the Node to Create Siblings
        void _node_split(blkptr_t);

        // Node Operations : Merge Sibling Nodes
        blkptr_t _node_merge(blkptr_internal_t, blkptr_t, blkptr_t);

        // Node Operations : steal keys clockwise
        void _node_steal_from_lsibling(blkptr_t&, blkptr_t&, blkptr_t&);

        // Node Operations : steal keys anticlockwise
        void _node_steal_from_rsibling(blkptr_t&, blkptr_t&, blkptr_t&);

        // Tree Ops : Get Leaf Node
        blkptr_leaf_t _tree_get_leaf_node(const bkey_t key, blkptr_t&);

        // Tree Ops : Get Internal Node
        blkptr_internal_t _tree_get_internal_node(const bkey_t key, blkptr_t&);

        // Tree Ops : relabance
        blkptr_t _tree_rebalance(blkptr_t curr);

        // Tree Ops : Insert Key
        void _tree_insert(const bkey_t, const mapping_t, blkptr_t&);

        // Tree Ops : Delete Key
        void _tree_delete(const index_t);

        // Tree Ops : LookUp
        blkptr_t _tree_lookup(const bkey_t key, blkptr_t node);

        // Tree Ops : Nuke tree
        void _tree_destroy(blkptr_t);

        // Tree Ops :  Print tree
        void _tree_print(const blkptr_t&) const;

        // Tree Ops : Tree traversal
        void _tree_level_traversal(const blkptr_t& node) const;

    public:

        void insert(const bkey_t, const mapping_t);

        void remove(const bkey_t);

        void print(void) const;

        blkptr_t lookup(const bkey_t);

        bptree(int);

       ~bptree();
};
#endif

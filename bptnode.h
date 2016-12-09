/*-------------------------------------------------
 * Copyright(C) 2016, Saptarshi Sen
 *
 * B+-Tree node definition
 *
 * -----------------------------------------------*/

#ifndef _BPTNODE_H
#define _BPTNODE_H

#include <iostream>
#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <map>
#include <vector>
#include <utility>
#include <memory>
#include "trace.h"

using namespace std;

typedef unsigned long long index_t;

#define LEAF(raw_node) static_pointer_cast<bptnode_leaf>(raw_node)
#define INTERNAL(raw_node) static_pointer_cast<bptnode_internal>(raw_node)

struct mapping_t {

    void* _base; //memory

    off_t _off; //on-disk

    size_t _size;

	mapping_t(void* base, unsigned long long offset, size_t bytes) :
        _base(base), _off(offset), _size(bytes) { }

	mapping_t() { }

       ~mapping_t() { }
};

typedef struct mapping_t mapping_t;

enum node_t { Root, Internal, Leaf };

class bptnode_raw : public enable_shared_from_this<bptnode_raw> {

	protected:

        shared_ptr<bptnode_raw> _parent;

        int _level;

	public:

	vector<index_t> _keys;

        node_t _type;

        index_t _max_cached, _min_cached;

	bptnode_raw(shared_ptr<bptnode_raw> parent, int level) : _parent(parent), _level(level) { }

        virtual ~bptnode_raw() {}

        shared_ptr<bptnode_raw> _parentp(void) const {
	        return _parent;
        }

        int _get_level(void) {
            return _level;
        }

        index_t _keysAt(int no) const {
	        if (no >= _keys.size())
		        throw exception();
	        return _keys.at(no);
        }

        int _separator(void) const {
	        return _keys.size()/2;
        }

	void set_parentp(shared_ptr<bptnode_raw> node) {
            _parent = node;
        }

        void reset_parentp(void) {
            _parent.reset();
        }

        void insert_key(const index_t key) {
            assert(std::find(_keys.begin(), _keys.end(), key) == _keys.end());
            _keys.push_back(key);

            sort(_keys.begin(), _keys.end(),
                 [] (const index_t& p, const index_t& q) { return (p < q);} );

            _min_cached = *_keys.begin();
            _max_cached = *_keys.rbegin();
        }

        void remove_key(const index_t key) {
	    _keys.erase(std::remove_if(_keys.begin(), _keys.end(),
                 [key] (const index_t& p) { return p == key; }), _keys.end());

            _min_cached = *_keys.begin();
            _max_cached = *_keys.rbegin();
        }

        int find_key(const index_t key) const {
	        auto it = std::find_if(_keys.begin(), _keys.end(),
                 [key] (const index_t p) { return p == key;});
  	        return (it != _keys.end()) ? std::distance(_keys.begin(), it) : -1;
        }

        void sort_node(vector<shared_ptr<bptnode_raw>>& node_list) {
            std::sort(node_list.begin(), node_list.end(),
                [] (shared_ptr<bptnode_raw> p, shared_ptr<bptnode_raw> q) {
                return (p->_min_cached < q->_min_cached);
                });
        }

        int _num_keys(void) {
            return _keys.size();
        }

        virtual int _num_child(void) = 0;

        virtual shared_ptr<bptnode_raw> _childAt(int) = 0;

};

class bptnode_internal : public bptnode_raw {

	protected:

	vector<shared_ptr<bptnode_raw>> _child;

	public:

        bptnode_internal(shared_ptr<bptnode_internal> parent, int branch):
            bptnode_raw(parent, branch) {
            _type = Internal;
        }

        ~bptnode_internal() {
            _keys.clear();
            _child.clear();
        }

        void insert_child(shared_ptr<bptnode_raw> node) {
            _child.push_back(node);
            sort_node(_child);
        }

        int find_child(shared_ptr<bptnode_raw>& node) {
	    auto it = std::find(_child.begin(), _child.end(), node);
	    return (it != _child.end()) ? std::distance(_child.begin(), it) : -1;
        }

        void remove_child(shared_ptr<bptnode_raw> node) {
	     _child.erase(std::remove(_child.begin(), _child.end(), node), _child.end());
	     node->reset_parentp();
        }

        int _num_child(void) {
             return _child.size();
        }

        shared_ptr<bptnode_raw> _childAt(int i) {
            if (i >= _child.size())
                throw exception();
            else
                return _child.at(i);
        }

	void print(void) {
	    trace_record(true, __func__, "level: ", _level, "<", _min_cached, ",", _max_cached, ">", 
                    "num keys: ", _num_keys(), " num child: ", _num_child());
        }    
};

class bptnode_leaf : public bptnode_raw {

	public:

	map<index_t, mapping_t> _kv;

        shared_ptr<bptnode_leaf> _prev, _next;

        bptnode_leaf(shared_ptr<bptnode_internal> parent, int branch):
            bptnode_raw(parent, branch) {
            _type = Leaf;
            _prev = _next = nullptr;
        }

        ~bptnode_leaf() {
            _keys.clear();
            _kv.clear();
        }

        shared_ptr<bptnode_leaf> next_record() {
            return _next;
        }

        void update_next_record(shared_ptr<bptnode_leaf> node) {
             _next = node;
        }

        void insert_record(const index_t key, const mapping_t& val) {
             assert(_kv.find(key) == _kv.end());
             _kv[key] = val;
             insert_key(key);
        }

        mapping_t& find_record(const index_t key) {
            if (_kv.find(key) == _kv.end())
                throw exception();
            return _kv[key];
         }

        void remove_record(const index_t key) {
             assert(_kv.find(key) != _kv.end());
             _kv.erase(key);
             remove_key(key);
        }

        int _num_child(void) {
             return 0;
        }

        shared_ptr<bptnode_raw> _childAt(int i) {
            throw exception();
        }

        void update_chain(shared_ptr<bptnode_leaf> prev,
                shared_ptr<bptnode_leaf> next) {

            //BUG FIX: do not assign shared pointer instances from 'this'

            if (prev) {
                _prev = prev;
                prev->_next = LEAF(shared_from_this());
            }    

            if (next) {
                _next = next;
                next->_prev = LEAF(shared_from_this());
            }    
        }

	void print(void) {
	    trace_record(true, __func__, "level: ", _level, "<", _min_cached, ",", _max_cached, ">", 
                    "num keys: ", _num_keys(), " num entry: ", _kv.size());
        }
};

#endif

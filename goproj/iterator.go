// Copyright (C) 2021 Saptarshi Sen

package goproj

import "log"

type Iterator struct {
	s          *Skiplist
	prev, curr *Node
	valid      bool
}

func (s *Skiplist) NewIterator() *Iterator {
	return &Iterator{
		s:     s,
		prev:  nil,
		curr:  s.head,
		valid: true,
	}
}

func (it *Iterator) isValid() bool {
	return it.curr != nil && it.curr != it.s.tail
}

func (it *Iterator) GetData() RawPointer {
	return it.curr.datap
}

func (it *Iterator) GetNode() *Node {
	return it.curr
}

func (it *Iterator) Next() {
	if it.valid {
		it.prev = it.curr
		node, deleted := it.curr.GetNext(0)
		for ; deleted; node, deleted = node.GetNext(0) {
		}
		it.curr = node
		if it.curr == it.s.tail {
			it.valid = false
		}
	}
}

func (it *Iterator) NextMutable() {
	if it.valid {
		node, deleted := it.curr.GetNext(0)
		if deleted {
			if !it.s.DeleteReal(0, it.prev, it.curr, node) {
				key := *(*int)(it.curr.datap)
				level := (int)(it.s.level)
				log.Println("iterator scan retrying delete, key:", key, "level:", level)
				pred := make([](*Node), level+1)
				succ := make([](*Node), level+1)
				it.s.FindPathV3(key, level, pred[:], succ[:])
				it.prev = pred[0]
				it.curr = succ[0]
			} else {
				it.prev = it.curr
				it.curr = node
			}
		} else {
			it.prev = it.curr
			it.curr = node
		}
		if it.curr == it.s.tail {
			it.valid = false
		}
	}
}

func (it *Iterator) Seek(ptr RawPointer) bool {
	level := (int)(it.s.level)
	key := *(*int)(ptr)
	pred := make([](*Node), level+1)
	succ := make([](*Node), level+1)
	found := it.s.FindPathV2(key, level, pred[:], succ[:])
	if found {
		node, deleted := pred[0].GetNext(0)
		if !deleted {
			it.prev = pred[0]
			it.curr = node
		} else {
			found = false
		}
	}
	return found
}

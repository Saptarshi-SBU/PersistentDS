// Copyright (c) 2021 Saptarshi Sen

package goproj

import (
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

var id uint64 = 0
var nr_insert_retries uint64 = 0

var dbg bool = false
var dbg_latency bool = false

type Skiplist struct {
	head  *Node
	tail  *Node
	level uint16
}

func NewSkiplist(level uint16) (s *Skiplist) {
	s = new(Skiplist)
	const MaxUint = ^uint(0)
	const MaxInt = int(MaxUint >> 1)
	const MinInt = -MaxInt - 1
	d := atomic.AddUint64(&id, 1)
	s.head = newNodeInternal(MinInt, level, d)
	d = atomic.AddUint64(&id, 1)
	s.tail = newNodeInternal(MaxInt, level, d)
	s.level = level
	for i := (uint16)(0); i < level; i++ {
		s.head.SetNext(i, s.tail, false)
		s.tail.SetNext(i, nil, false)
	}
	if dbg {
		log.Println("skiplist head node:", s.head)
		log.Println("skiplist tail node:", s.tail)
	}
	return s
}

func (s *Skiplist) GetNewLevelPoorMan() uint16 {
	r := (uint16)(rand.Intn((int)(s.level)))
	if r == 0 {
		return 1
	} else {
		return r
	}
}

func (s *Skiplist) GetNewLevel() uint16 {
	var nextLevel uint16 = 1
	for ; nextLevel < s.level && (rand.Float32() < 0.25); nextLevel++ {
	}
	return nextLevel
}

func (s *Skiplist) InsertConcurrentUnsafe(ptr RawPointer) {
	var i uint16
	v := *(*int)(ptr)
	l := s.GetNewLevel()
	node := newNodeData(ptr, l, 0)
	if dbg {
		log.Println("inserting node to skiplist:", node, "value:", *(*int)(node.datap))
	}
	for i = 0; i < l; i++ {
		var prev *Node
		for curr := s.head; curr != nil; curr, _ = curr.GetNext(i) {
			d := *(*int)(curr.datap)
			if v < d {
				prev.SetNext(i, node, false)
				node.SetNext(i, curr, false)
				break
			}
			prev = curr
		}
	}
}

func (s *Skiplist) InsertConcurrentSimple(ptr RawPointer, goid uint64) {
	//var found bool
	l := s.GetNewLevel()
	v := *(*int)(ptr)
	node := newNodeData(ptr, l, goid)
	t0 := time.Now()
	for i := (uint16)(0); i < l; i++ {
		d := 0
		p := 0
		prev := (*Node)(nil)
		curr := (*Node)(nil)
	retry:
		for curr = s.head; curr != nil; curr, _ = curr.GetNext(i) {
			d = *(*int)(curr.datap)
			if prev != nil {
				p = *(*int)(prev.datap)
			}
			if v < d {
				node.SetNext(i, curr, false)
				swapped := prev.UpdateNext(i, curr, node, false, false)
				if swapped {
					if dbg {
						log.Println("->insert:", goid, p, v, d)
					}
					break
				}
				prev = nil
				if dbg {
					log.Println("insert conflict:", v)
				}
				goto retry
			}
			prev = curr
		}
		if dbg {
			log.Println("<-insert:", goid, p, v, d, prev, curr)
		}
	}
	if dbg_latency {
		log.Println("insert duration :", time.Since(t0))
	}
}

func (s *Skiplist) FindPathV1(key int, level int) (prev, next *Node) {
	begin := s.head
	prev = (*Node)(nil)
	next = (*Node)(nil)
	for i := (int)(s.level); i >= level; i-- {
		for curr := begin; curr != nil; curr, _ = curr.GetNext((uint16)(i)) {
			data := *(*int)(curr.datap)
			if key < data {
				begin = prev
				next = curr
				break
			}
			prev = curr
		}
	}
	if dbg {
		if prev != nil {
			log.Println("key:", key, "neighbours:", *(*int)(prev.datap),
				*(*int)(next.datap), "level:", level, "max level:", s.level)
		} else {
			log.Println("key:", key, "neighbours:", prev, next,
				level, s.level)
		}
	}
	return prev, next
}

func (s *Skiplist) FindPathV2(key int, level int, pred [](*Node), succ [](*Node)) bool {
	prev := s.head
	found := false
	for i := (int)(s.level) - 1; i >= 0; i-- {
		for curr := prev; curr != nil; curr, _ = curr.GetNext((uint16)(i)) {
			data := *(*int)(curr.datap)
			if key < data {
				pred[i] = prev
				succ[i] = curr
				break
			} else if key == data {
				pred[i] = prev
				succ[i], _ = curr.GetNext((uint16)(i))
				found = true
				break
			}
			prev = curr
		}
	}
	if dbg {
		for i := (int)(s.level) - 1; i >= 0; i-- {
			log.Println("key:", key, "neighbours:", *(*int)(pred[i].datap),
				*(*int)(succ[i].datap), "level:", i, level)
		}
	}
	return found
}

// The function can delete stale nodes if present in a path lookup
func (s *Skiplist) FindPathV3(key int, level int, pred [](*Node), succ [](*Node)) bool {
retry:
	prev := s.head
	found := false
	deleted := false
	pprev := (*Node)(nil)
	flushed := false
	// irrespective of node level, we start scanning from the maximum skiplist node level
	for i := (int)(s.level) - 1; i >= 0; i-- {
		for curr := prev; curr != nil; curr, deleted = curr.GetNext((uint16)(i)) {
			// flush stale items
			for deleted {
				var next *Node
				if !s.DeleteReal(i, pprev, prev, curr) { // someone might have unlinked the node
					log.Println("retrying delete for key:", key,
						"real key:", *(*int)(prev.datap))
					goto retry
				}
				next, deleted = curr.GetNext((uint16)(i))
				if deleted {
					prev = curr
					curr = next
				} else {
					prev = pprev
				}
				flushed = true
			}

			// if items are flushed, we need to restart the findPath because we might be
			// diverted while deleting nodes
			if flushed {
				goto retry
			}
			data := *(*int)(curr.datap)
			if key < data {
				pred[i] = prev
				succ[i] = curr
				break
			} else if key == data {
				pred[i] = prev
				succ[i], _ = curr.GetNext((uint16)(i))
				found = true
				break
			}
			pprev = prev
			prev = curr
		}
	}
	if dbg {
		for i := (int)(s.level) - 1; i >= 0; i-- {
			log.Println("key:", key, "neighbours:", *(*int)(pred[i].datap),
				*(*int)(succ[i].datap), "level:", i, level)
		}
	}
	return found
}

func (s *Skiplist) GetConcurrent(key int64) *Node {
	level := s.level
	pred := make([](*Node), level+1)
	succ := make([](*Node), level+1)
retry:
	found := s.FindPathV3((int)(key), 0, pred, succ)
	if found {
		next, deleted := pred[0].GetNext(0)
		if !deleted {
			if key == *(*int64)(next.Item()) {
				return next
			} else {
				goto retry
			}
		}
	}
	return (*Node)(nil)
}

func (s *Skiplist) InsertConcurrentV1(ptr RawPointer) {
	level := s.GetNewLevel()
	key := *(*int)(ptr)
	node := newNodeData(ptr, level, 0)
	t0 := time.Now()
	for i := 0; i < (int)(level); i++ {
	retry:
		prev, next := s.FindPathV1(key, i)
		node.SetNext((uint16)(i), next, false)
		swapped := prev.UpdateNext((uint16)(i), next, node, false, false)
		if swapped {
			if dbg {
				p := *(*int)(prev.datap)
				d := *(*int)(next.datap)
				log.Println("insert:", p, key, d)
			}
		} else {
			goto retry
		}
	}
	if dbg_latency {
		log.Println("insert duration :", time.Since(t0))
	}
}

func (s *Skiplist) InsertConcurrentV2(ptr RawPointer) {
	level := s.GetNewLevel()
	key := *(*int)(ptr)
	node := newNodeData(ptr, level, 0)
	pred := make([](*Node), s.level)
	succ := make([](*Node), s.level)
	t0 := time.Now()
	_ = s.FindPathV2(key, (int)(level), pred[:], succ[:])
	for i := 0; i < (int)(level); i++ {
	retry:
		node.SetNext((uint16)(i), succ[i], false)
		swapped := pred[i].UpdateNext((uint16)(i), succ[i], node, false, false)
		if swapped {
			if dbg {
				p := *(*int)(pred[i].datap)
				d := *(*int)(succ[i].datap)
				log.Println("insert:", p, key, d)
			}
		} else {
			_ = s.FindPathV2(key, (int)(i), pred[:], succ[:])
			atomic.AddUint64(&nr_insert_retries, 1)
			//log.Println("retry insert", pred[i], succ[i], i)
			goto retry
		}
	}
	if dbg_latency {
		log.Println("insert duration :", time.Since(t0))
	}
}

// The deletion happens in two phases.
// Phase 1 : Soft Delete
// Phase 2 : Unlink
func (s *Skiplist) DeleteConcurrent(ptr RawPointer) bool {
	key := *(*int)(ptr)
	level := (int)(s.level)
	if s.DeleteSoft(ptr) {
		pred := make([](*Node), level+1)
		succ := make([](*Node), level+1)
		if dbg {
			log.Println("attempt real delete after soft deletion, key:", key)
		}
		// for updating link, we need to fetch the path from the node
		// +1 because we want to delete the node
		s.FindPathV3(key+1, level, pred[:], succ[:])
		return true
	} else {
		log.Println("failed to mark for soft deletion, key:", key)
		return false
	}
}

// This stamps the node's level pointers with the deletedFlag
func (s *Skiplist) DeleteSoft(ptr RawPointer) bool {
	i := 0
	key := *(*int)(ptr)
	level := (int)(s.level)
	pred := make([](*Node), level+1)
	succ := make([](*Node), level+1)
	if dbg {
		log.Println("marking key for soft deletion:", key)
	}
retry:
	found := s.FindPathV3(key, i, pred[:], succ[:])
	if !found {
		log.Println("key not present in Skiplist for soft deletion, key:", key)
		return false
	}
	node, _ := pred[i].GetNext((uint16)(i))
	for ; i < (int)(node.Level()); i++ {
		if !node.UpdateNext((uint16)(i), succ[i], succ[i], false, true) {
			log.Println("retrying soft deletion, key:", key, "level:", i)
			goto retry
		}
	}
	if dbg {
		log.Println("key is soft deleted:", key)
	}
	return true
}

// Real unlink happens here using an atomic update of the forward pointers
func (s *Skiplist) DeleteReal(level int, pred, node, succ *Node) bool {
	if pred == nil || node == nil {
		panic("nil nodes passed to DeleteReal")
	}
	key := *(*int)(node.datap)
	if dbg {
		log.Println("deleting key real:", key, "level:", level)
	}
	_, deleted := node.GetNext((uint16)(level))
	if !deleted {
		log.Println("cannot unlink, node not marked for deletion:", key)
		return false
	}
	if !pred.UpdateNext((uint16)(level), node, succ, false, false) {
		log.Println("failed to unlink node due to a race", key, "level:", level)
		return false
	}
	if dbg {
		log.Println("node removed from Skiplist, key:", key, "level:", level)
	}
	return true
}

func (s *Skiplist) Print() {
	for i := (uint16)(0); i < s.level; i++ {
		c := -2
		fmt.Println("traversing skiplist level:", i, s.head, s.tail)
		for n := s.head; n != nil; n, _ = n.GetNext(i) {
			if dbg {
				log.Println("node value:", n.datap, *(*int)(n.datap))
			}
			c = c + 1
		}
		log.Println("count items at level", i, ":", c)
	}
}

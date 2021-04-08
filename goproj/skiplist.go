// Copyright (c) 2021 Saptarshi Sen

package goproj

import (
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
)

var id uint64 = 0

var dbg bool = false

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
	log.Println("skiplist head node:", s.head)
	log.Println("skiplist tail node:", s.tail)
	return s
}

func (s *Skiplist) GetNewLevel() uint16 {
	r := (uint16)(rand.Intn((int)(s.level)))
	if r == 0 {
		return 1
	} else {
		return r
	}
}

func (s *Skiplist) InsertNoThreadSafe(ptr RawPointer) {
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

func (s *Skiplist) InsertThreadSafe(ptr RawPointer, goid uint64) {
	//var found bool
	l := s.GetNewLevel()
	v := *(*int)(ptr)
	node := newNodeData(ptr, l, goid)
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

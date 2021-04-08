// Copyright (c) 2021 Saptarshi Sen

package goproj

import (
	"log"
	"reflect"
	"sync/atomic"
	"unsafe"
)

type RawPointer = unsafe.Pointer

var deletedFlag int16 = 0x1

type Item struct {
	val int64
}

type Node struct {
	datap unsafe.Pointer
	id    uint64
	Cache int64
	level uint16
}

type NodeRef struct {
	flag int64
	ptr  *Node
}

var nodeTypes = [33]reflect.Type{
	reflect.TypeOf(node0),
	reflect.TypeOf(node1),
	reflect.TypeOf(node2),
	reflect.TypeOf(node3),
	reflect.TypeOf(node4),
	reflect.TypeOf(node5),
	reflect.TypeOf(node6),
	reflect.TypeOf(node7),
	reflect.TypeOf(node8),
	reflect.TypeOf(node9),
	reflect.TypeOf(node10),
	reflect.TypeOf(node11),
	reflect.TypeOf(node12),
	reflect.TypeOf(node13),
	reflect.TypeOf(node14),
	reflect.TypeOf(node15),
	reflect.TypeOf(node16),
	reflect.TypeOf(node17),
	reflect.TypeOf(node18),
	reflect.TypeOf(node19),
	reflect.TypeOf(node20),
	reflect.TypeOf(node21),
	reflect.TypeOf(node22),
	reflect.TypeOf(node23),
	reflect.TypeOf(node24),
	reflect.TypeOf(node25),
	reflect.TypeOf(node26),
	reflect.TypeOf(node27),
	reflect.TypeOf(node28),
	reflect.TypeOf(node29),
	reflect.TypeOf(node30),
	reflect.TypeOf(node31),
	reflect.TypeOf(node32),
}

var node0 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [1]NodeRef
}

var node1 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [2]NodeRef
}

var node2 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [3]NodeRef
}

var node3 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [4]NodeRef
}

var node4 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [5]NodeRef
}

var node5 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [6]NodeRef
}

var node6 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [7]NodeRef
}

var node7 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [8]NodeRef
}

var node8 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [9]NodeRef
}

var node9 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [10]NodeRef
}

var node10 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [11]NodeRef
}
var node11 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [12]NodeRef
}

var node12 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [13]NodeRef
}

var node13 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [14]NodeRef
}

var node14 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [15]NodeRef
}

var node15 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [16]NodeRef
}

var node16 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [17]NodeRef
}

var node17 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [18]NodeRef
}

var node18 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [19]NodeRef
}

var node19 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [20]NodeRef
}

var node20 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [21]NodeRef
}

var node21 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [22]NodeRef
}

var node22 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [23]NodeRef
}

var node23 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [24]NodeRef
}

var node24 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [25]NodeRef
}

var node25 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [26]NodeRef
}

var node26 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [27]NodeRef
}

var node27 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [28]NodeRef
}

var node28 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [29]NodeRef
}

var node29 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [30]NodeRef
}

var node30 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [31]NodeRef
}
var node31 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [32]NodeRef
}

var node32 struct {
	itm   unsafe.Pointer
	id    uint64
	cache int64
	buf   [33]NodeRef
}

// return level of each node
func (n Node) Level() int {
	return int(n.level)
}

// skiplist base node size excluding the levels
var nodeHdrSize = unsafe.Sizeof(
	struct {
		datap RawPointer
		id    uint64
		Cache int64
	}{})

// skiplist per level pointer
var nodeRefSize = unsafe.Sizeof(NodeRef{})

var nodeRefFlagSize = unsafe.Sizeof(NodeRef{}.flag)

// returns total size of the skiplist node
func (n Node) Size() int {
	return int(nodeHdrSize + uintptr(n.level+1)*nodeRefSize)
}

// returns data pointer
func (n *Node) Item() RawPointer {
	return n.datap
}

// sets data pointer
func (n *Node) SetItem(datap RawPointer) {
	n.datap = datap
}

// returns next node in a level
func (n *Node) GetNext(level uint16) (*Node, bool) {
	nodeRefAddr := uintptr(RawPointer(n)) + nodeHdrSize + uintptr(level)*nodeRefSize
	wordAddr := (*uint64)(RawPointer(nodeRefAddr + uintptr(7)))
	v := atomic.LoadUint64(wordAddr)
	deleted := (v & (uint64)(deletedFlag)) == (uint64)(deletedFlag)
	ptr := (*Node)(RawPointer(uintptr(v >> 8)))
	if dbg {
		log.Println("GetNext:", "level:", level, "node:", n, "next:", ptr)
	}
	return ptr, deleted
}

// set next node
func (n *Node) SetNext(level uint16, ptr *Node, deleted bool) {
	nodeRefAddr := uintptr(RawPointer(n)) + nodeHdrSize + uintptr(level)*nodeRefSize
	ref := (*NodeRef)(RawPointer(nodeRefAddr))
	if deleted {
		ref.flag = (int64)(deletedFlag)
	}
	if dbg {
		log.Println("SetNext:", "level:", level, "node:", n, "next:", ptr)
	}
	ref.ptr = ptr
}

// update next node
func (n *Node) UpdateNext(level uint16, prevPtr, newPtr *Node, prevDeleted, newDeleted bool) bool {
	nodeRefAddr := uintptr(RawPointer(n)) + nodeHdrSize + uintptr(level)*nodeRefSize
	wordAddr := (*uint64)(RawPointer(nodeRefAddr + uintptr(7)))
	prevVal := (uint64)(uintptr(RawPointer(prevPtr)) << 8)
	newVal := (uint64)(uintptr(RawPointer(newPtr)) << 8)
	/*
		if prevVal == 0 {
			panic("nil ptr detected")
		}
		if newVal == 0 {
			panic("nil ptr detected")
		}
	*/
	if prevDeleted {
		prevVal |= (uint64)(deletedFlag)
	}
	if newDeleted {
		newVal |= (uint64)(deletedFlag)
	}
	swapped := atomic.CompareAndSwapUint64(wordAddr, prevVal, newVal)
	if swapped {
		atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(nodeRefAddr+nodeRefFlagSize)),
			unsafe.Pointer(newPtr), unsafe.Pointer(newPtr))
	}
	return swapped
}

// allocate a node
func newNodeInternal(val int, level uint16, idx uint64) *Node {
	node := (*Node)(RawPointer(reflect.New(nodeTypes[level]).Pointer()))
	data := new(int)
	*data = val
	node.datap = RawPointer(data)
	node.level = level
	node.id = idx
	if dbg {
		log.Println("new internal node:", *(*int)(node.datap))
	}
	return node
}

func newNodeData(val RawPointer, level uint16, idx uint64) *Node {
	node := (*Node)(RawPointer(reflect.New(nodeTypes[level]).Pointer()))
	node.datap = val
	node.level = level
	node.id = idx
	if dbg {
		log.Println("new item:", *(*int)(node.datap))
	}
	return node
}

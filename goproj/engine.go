// Copyright (c) 2021 Saptarshi Sen

package goproj

import (
	"log"
	"sync/atomic"
)

var dbg_engine bool = false

type StorageEngine struct {
	i_list       *Skiplist
	s_list       *Skiplist
	curr_version int64
	gc_channel   chan int64
}

func InitializeStorageEngine() *StorageEngine {
	s := &StorageEngine{
		i_list:       NewSkiplist(4),
		s_list:       NewSkiplist(4),
		curr_version: 1,
		gc_channel:   make(chan int64),
	}
	go gcWorker(s)
	return s
}

func (s *StorageEngine) GetVersion() int64 {
	return s.curr_version
}

func (s *StorageEngine) NewSnapShot() *Snapshot {
	next_vers := atomic.AddInt64(&s.curr_version, 1)
	snap := &Snapshot{
		version:  next_vers - 1,
		refcount: 0,
		i_list:   s.i_list,
	}
	s.InsertSnapshot(snap)
	return snap
}

func (s *StorageEngine) InsertSnapshot(snap *Snapshot) {
	atomic.AddInt64(&snap.refcount, 1)
	s.s_list.InsertConcurrentV2(RawPointer(snap))
}

func (s *StorageEngine) GetSnapshot(snap_id int64) *Snapshot {
	node := s.s_list.GetConcurrent(snap_id)
	if node == nil {
		log.Println("entry not found for snapshot id:", snap_id)
	}
	return (*Snapshot)(node.datap)
}

func (s *StorageEngine) DeleteSnapshot(snap *Snapshot) {
	if atomic.AddInt64(&snap.refcount, -1) == 0 {
		s.s_list.DeleteConcurrent(RawPointer(snap))
		// trigger garbage collector
		s.gc_channel <- snap.version
	}
}

func (s *StorageEngine) VisitSnapshots() {
	it := s.s_list.NewIterator()
	log.Println("scanning snapshots:")
	for it.NextMutable(); it.isValid(); it.NextMutable() {
		snap := *(*Snapshot)(it.GetData())
		log.Println("visited snapshot id:", snap.version)
	}
}

func (s *StorageEngine) InsertKey(val int64) {
	curr_vers := s.GetVersion()
	item := AllocateItem(val, curr_vers)
	s.i_list.InsertConcurrentV2(RawPointer(item))
	if dbg_engine {
		log.Println("inserted item key:", item, item.GetItemValue())
	}
}

func (s *StorageEngine) DeleteKey(val int64) {
	curr_vers := s.GetVersion()
	node := s.i_list.GetConcurrent(val)
	item := (*Item)(node.datap)
	item_meta := item.GetItemMetaInfo()
	item.DeleteItem(curr_vers)
	// No older shapshots are currently referring this item
	if item_meta.born_vers == curr_vers {
		s.i_list.DeleteConcurrent(RawPointer(item))
	}
	if dbg_engine {
		log.Println("deleted item key:", item)
	}
}

func (s *StorageEngine) VisitItems() {
	it := s.i_list.NewIterator()
	log.Println("scanning items:")
	for it.NextMutable(); it.isValid(); it.NextMutable() {
		item := *(*Item)(it.GetData())
		log.Println("visited key:", item.GetItemValue())
	}
}

func gcWorker(s *StorageEngine) {
	for {
		select {
		case snap_key := <-s.gc_channel:
			if snap_key < 0 {
				s.gc_channel <- 0
				return
			}
			s.runGC(snap_key)
		}
	}
}

func (s *StorageEngine) runGC(snap_key int64) {
	gc_items := 0
	prev := (*Node)(nil)
	it := s.i_list.NewIterator()
	log.Println("running garbage collector, current version:",
		s.curr_version, "snapshot_id:", snap_key)
	for it.NextMutable(); it.isValid(); it.NextMutable() {
		if prev != nil {
			item := (*Item)(prev.Item())
			if s.CheckGCCandidate(item, snap_key) {
				key := item.GetItemValue()
				s.i_list.DeleteConcurrent(RawPointer(&key))
				gc_items++
			}
		}
		prev = it.GetNode()
		//log.Println("prev:", prev)
		//time.Sleep(time.Duration(time.Second))
	}

	if prev != nil {
		item := (*Item)(prev.Item())
		if s.CheckGCCandidate(item, snap_key) {
			key := item.GetItemValue()
			s.i_list.DeleteConcurrent(RawPointer(&key))
			gc_items++
		}
		prev = nil
	}
	log.Println("items garbage collected:", gc_items)
}

func (s *StorageEngine) CheckGCCandidate(item *Item, snap_id int64) bool {
	found := true
	meta := item.GetItemMetaInfo()
	if meta.dead_vers <= snap_id {
		found = s.s_list.GetRangeConcurrent((int)(meta.born_vers), (int)(meta.dead_vers))
	}
	if dbg_engine {
		log.Println("item info:", meta, "gc candidate:", !found)
	}
	return !found
}

func (s *StorageEngine) ShutdownGC() {
	// force last iteration of GC for any items which are deleted and
	// not covered under a snapshot
	s.gc_channel <- (MaxInt64 - 1)
	s.gc_channel <- -1
	<-s.gc_channel
}

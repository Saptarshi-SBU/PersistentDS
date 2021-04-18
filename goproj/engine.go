// Copyright (c) 2021 Saptarshi Sen

package goproj

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"unsafe"
)

var dbg_engine bool = false

type StorageEngine struct {
	i_list       *Skiplist
	s_list       *Skiplist
	curr_version int64
	gc_channel   chan int64
}

type IOWriter struct {
	file *os.File
	w    *bufio.Writer
}

type IOReader struct {
	file *os.File
	r    *bufio.Reader
}

// disk item meta data
type item_hdr struct {
	crc uint32
	len uint32
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

func ShutdownStorageEngine(s *StorageEngine) {
	s.gc_channel = nil
	s.s_list = nil
	s.i_list = nil
}

func NewWriter(filename string) *IOWriter {
	file, err := os.Create(filename)
	if err != nil {
		panic("error opening db file:")
	}
	rw := &IOWriter{
		file: file,
		w:    bufio.NewWriter(file),
	}
	return rw
}

func NewReader(filename string) *IOReader {
	file, err := os.Open(filename)
	if err != nil {
		panic("error opening db file:")
	}
	rw := &IOReader{
		file: file,
		r:    bufio.NewReader(file),
	}
	return rw
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

func (s *StorageEngine) SaveSnapshot(snap *Snapshot) {
	cb := 0
	it := s.i_list.NewIterator()
	log.Println("saving snapshot version:", snap.version)
	rw := NewWriter("db" + "-" + "snap" + strconv.Itoa((int)(snap.version)) + ".dump")
	for it.Next(); it.isValid(); it.Next() {
		item := (*Item)(it.GetData())
		meta := item.GetItemMetaInfo()
		if meta.born_vers <= snap.version && meta.dead_vers > snap.version {
			val := item.GetItemValue()
			// write header
			hdr := item_hdr{crc: 0, len: 0}
			hdr.len = (uint32)(unsafe.Sizeof(val))
			total_bytes := (uint32)(unsafe.Sizeof(hdr)) + hdr.len
			buf := make([]byte, total_bytes)
			binary.BigEndian.PutUint32(buf[4:8], hdr.len)
			binary.BigEndian.PutUint64(buf[8:total_bytes], (uint64)(val))
			hdr.crc = crc32.ChecksumIEEE(buf[8:total_bytes])
			binary.BigEndian.PutUint32(buf[0:4], hdr.crc)
			nn, err := rw.w.Write(buf)
			if err != nil {
				panic("error writing item!!!")
			}
			log.Println("serialized item to disk:", val)
			if dbg_engine {
				log.Println("bytes:", nn, "crc:", hdr.crc, "nlength:", hdr.len)
			}
			cb = cb + nn
		}
	}
	log.Println("total bytes written to disk:", cb)
	rw.w.Flush()
}

func (s *StorageEngine) LoadSnapshot(snap_version int64) {
	cb := 0
	rw := NewReader("db" + "-" + "snap" + strconv.Itoa((int)(snap_version)) + ".dump")
	hdr := item_hdr{crc: 0, len: 0}
	hdr_buf := make([]byte, unsafe.Sizeof(hdr))
	for {
		var hdr item_hdr
		nn, err := io.ReadFull(rw.r, hdr_buf)
		if nn == 0 {
			break
		}
		if (err != nil) || (nn != len(hdr_buf)) {
			log.Println("error reading item header!!!")
			break
		}
		cb = cb + nn
		hdr.crc = binary.BigEndian.Uint32(hdr_buf[0:4])
		hdr.len = binary.BigEndian.Uint32(hdr_buf[4:8])
		if dbg_engine {
			log.Println("item meta data ", "crc:", hdr.crc, "nlength:", hdr.len)
		}
		data_buf := make([]byte, hdr.len)
		nn, err = io.ReadFull(rw.r, data_buf)
		if (err != nil) || (nn != len(data_buf)) {
			log.Println("error reading item data!!!")
			break
		}
		cb = cb + nn
		val := binary.BigEndian.Uint64(data_buf)
		crc := crc32.ChecksumIEEE(data_buf)
		if crc != hdr.crc {
			log.Println("read data checksum error!!!", crc)
			break
		}
		log.Println("deserialized item from disk:", val)
		s.InsertKey((int64)(val))
	}
	log.Println("total bytes read from disk:", cb)
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

func (s *StorageEngine) VisitItems() uint64 {
	var count uint64 = 0
	it := s.i_list.NewIterator()
	log.Println("scanning items:")
	for it.NextMutable(); it.isValid(); it.NextMutable() {
		count = count + 1
		item := *(*Item)(it.GetData())
		log.Println("visited key:", item.GetItemValue())
	}
	return count
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
		found = s.s_list.GetRangeConcurrent((int)(meta.born_vers),
			(int)(meta.dead_vers))
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

// Copyright (c) 2021 Saptarshi Sen
package goproj

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

var count = flag.String("count", "", "dir of package containing embedded files")

func TestSkiplist(t *testing.T) {
	s := NewSkiplist(4)
	s.Print()
}

func TestSkiplistInsert(t *testing.T) {
	s := NewSkiplist(8)
	for i := 0; i < 1000; i++ {
		p := new(int)
		*p = rand.Intn(1000)
		s.InsertConcurrentUnsafe(RawPointer(p))
	}
	s.Print()
}

func TestSkiplistInsertConcurrentSimple(t *testing.T) {
	s := NewSkiplist(4)
	var wg sync.WaitGroup
	for i := 0; i < 1024; i++ {
		wg.Add(1)
		go func() {
			p := new(int)
			*p = rand.Intn(1000)
			s.InsertConcurrentSimple(RawPointer(p), (uint64)(i))
			wg.Done()
		}()
	}
	wg.Wait()
	s.Print()
}

func TestSkiplistInsertConcurrentSimpleGC(t *testing.T) {
	var ops uint64
	var wg sync.WaitGroup
	n := 200000
	ops = 0
	s := NewSkiplist(4)
	fmt.Println("inserting items:", n)
	gc := debug.SetGCPercent(100)
	fmt.Println("GC (%):", gc)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			p := new(int)
			*p = rand.Intn(1000)
			s.InsertConcurrentSimple(RawPointer(p), (uint64)(i))
			atomic.AddUint64(&ops, 1)
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Println("inserted ops:", ops)
	s.Print()
}

func TestSkiplistInsertConcurrentSimpleNoGC(t *testing.T) {
	var ops uint64
	var wg sync.WaitGroup
	var stats debug.GCStats
	n := 1800
	ops = 0
	fmt.Println("inserting items:", n)
	// disable garbage collection
	gc := debug.SetGCPercent(-1)
	fmt.Println("GC (%):", gc)
	s := NewSkiplist(4)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			p := new(int)
			*p = rand.Intn(1000)
			s.InsertConcurrentSimple(RawPointer(p), (uint64)(i))
			atomic.AddUint64(&ops, 1)
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Println("inserted ops:", ops)
	debug.ReadGCStats(&stats)
	fmt.Println("GC (%):", debug.SetGCPercent(0))
	fmt.Println("num gc:", stats.NumGC, "gctime:", stats.PauseTotal)
	s.Print()
}

func doInsertSimple(s *Skiplist, ptr RawPointer, wg *sync.WaitGroup, goid int) {
	defer wg.Done()
	s.InsertConcurrentSimple(ptr, (uint64)(goid))
}

func doInsertV0(s *Skiplist, wg *sync.WaitGroup, n int, isRand bool) {
	defer wg.Done()
	var val int
	if isRand {
		rnd := rand.New(rand.NewSource(int64(rand.Int())))
		val = rnd.Int()
	} else {
		val = n
	}
	itm := int(val)
	s.InsertConcurrentSimple(unsafe.Pointer(&itm), (uint64)(1))
}

func doInsertV1(s *Skiplist, wg *sync.WaitGroup, n int, isRand bool) {
	defer wg.Done()
	var val int
	if isRand {
		rnd := rand.New(rand.NewSource(int64(rand.Int())))
		val = rnd.Int()
	} else {
		val = n
	}
	itm := int(val)
	s.InsertConcurrentV1(unsafe.Pointer(&itm))
}

func doInsertV2(s *Skiplist, wg *sync.WaitGroup, n int, isRand bool) {
	defer wg.Done()
	var val int
	if isRand {
		rnd := rand.New(rand.NewSource(int64(rand.Int())))
		val = rnd.Int()
	} else {
		val = n
	}
	itm := int(val)
	s.InsertConcurrentV2(unsafe.Pointer(&itm))
}

func doDelete(s *Skiplist, wg *sync.WaitGroup, n int, isRand bool) {
	defer wg.Done()
	itm := int(n)
	s.DeleteConcurrent(unsafe.Pointer(&itm))
	fmt.Println("deleted :", n)
}

func TestSkiplistInsertConcurrentSimpleSlice(t *testing.T) {
	var wg sync.WaitGroup
	var stats debug.GCStats
	n := 600000
	fmt.Println("inserting items:", n)
	gc := debug.SetGCPercent(100)
	fmt.Println("GC (%):", gc)
	s := NewSkiplist(4)
	p := make([]int, n+1)
	for i := 0; i < n; i++ {
		p[i] = i
		wg.Add(1)
		go doInsertSimple(s, RawPointer(&p[i]), &wg, i)
	}
	wg.Wait()
	debug.ReadGCStats(&stats)
	fmt.Println("GC (%):", debug.SetGCPercent(100))
	fmt.Println("num gc:", stats.NumGC, "gctime:", stats.PauseTotal)
	s.Print()
}

func TestSkiplistInsertValuesV0(t *testing.T) {
	var wg sync.WaitGroup
	var stats debug.GCStats
	n := 100000
	fmt.Println("inserting items:", n)
	gc := debug.SetGCPercent(100)
	fmt.Println("GC (%):", gc)
	s := NewSkiplist(4)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go doInsertV0(s, &wg, i, false)
	}
	wg.Wait()
	debug.ReadGCStats(&stats)
	fmt.Println("GC (%):", debug.SetGCPercent(100))
	fmt.Println("num gc:", stats.NumGC, "gctime:", stats.PauseTotal)
	s.Print()
}

func TestSkiplistInsertValuesV1(t *testing.T) {
	var wg sync.WaitGroup
	n := 100000
	s := NewSkiplist(4)
	t0 := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go doInsertV1(s, &wg, i, false)
	}
	wg.Wait()
	dur := time.Since(t0)
	s.Print()
	fmt.Printf("%d items took %v insert conflicts :%v\n", n, dur, nr_insert_retries)
}

func TestSkiplistInsertValuesV2(t *testing.T) {
	var wg sync.WaitGroup
	n, _ := strconv.Atoi(*count)
	s := NewSkiplist(4)
	t0 := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go doInsertV2(s, &wg, i, false)
	}
	wg.Wait()
	dur := time.Since(t0)
	s.Print()
	fmt.Printf("%d items took %v insert conflicts :%v\n", n, dur, nr_insert_retries)
}

func TestSkipListIterator(t *testing.T) {
	var wg sync.WaitGroup
	n := 64
	s := NewSkiplist(4)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go doInsertV2(s, &wg, i, false)
	}
	wg.Wait()
	it := s.NewIterator()
	for it.Next(); it.isValid(); it.Next() {
		data := *(*int)(it.GetData())
		log.Println("node val:", it.GetNode(), data)
	}
	s.Print()
}

func TestSkipListIteratorwithSeek(t *testing.T) {
	var wg sync.WaitGroup
	n := 64
	s := NewSkiplist(4)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go doInsertV2(s, &wg, i, false)
	}
	wg.Wait()
	it := s.NewIterator()
	item := int(32)
	found := it.Seek(RawPointer(&item))
	if found {
		for ; it.isValid(); it.Next() {
			data := *(*int)(it.GetData())
			log.Println("node val:", it.GetNode(), data)
		}
	}
	s.Print()
}

func TestSkipListIteratorwithDeletes(t *testing.T) {
	var wg_insert, wg_delete sync.WaitGroup
	n := 32
	s := NewSkiplist(4)
	for i := 0; i < n; i++ {
		wg_insert.Add(1)
		go doInsertV2(s, &wg_insert, i, false)
	}
	wg_insert.Wait()
	for i := 0; i < n/2; i++ {
		wg_delete.Add(1)
		go doDelete(s, &wg_delete, i, false)
	}
	wg_delete.Wait()
	it := s.NewIterator()
	log.Println("iterator scan:")
	for it.NextMutable(); it.isValid(); it.NextMutable() {
		data := *(*int)(it.GetData())
		log.Println("scan val:", it.GetNode(), data)
	}
}

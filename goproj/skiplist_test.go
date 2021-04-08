// Copyright (c) 2021 Saptarshi Sen
package goproj

import (
	"fmt"
	"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestSkiplist(t *testing.T) {
	s := NewSkiplist(4)
	s.Print()
}

func TestSkiplistInsert(t *testing.T) {
	s := NewSkiplist(8)
	for i := 0; i < 1000; i++ {
		p := new(int)
		*p = rand.Intn(1000)
		s.InsertNoThreadSafe(RawPointer(p))
	}
	s.Print()
}

func TestSkiplistInsertAtomics(t *testing.T) {
	s := NewSkiplist(4)
	for i := 0; i < 8; i++ {
		p := new(int)
		*p = rand.Intn(1000)
		s.InsertThreadSafe(RawPointer(p), 0)
	}
	s.Print()
}

func TestSkiplistInsertAtomicsAsync(t *testing.T) {
	s := NewSkiplist(4)
	var wg sync.WaitGroup
	for i := 0; i < 1024; i++ {
		wg.Add(1)
		go func() {
			p := new(int)
			*p = rand.Intn(1000)
			s.InsertThreadSafe(RawPointer(p), (uint64)(i))
			wg.Done()
		}()
	}
	wg.Wait()
	s.Print()
}

func TestSkiplistInsertAtomicsAsyncBig(t *testing.T) {
	var ops uint64
	var wg sync.WaitGroup
	n := 200000
	ops = 0
	s := NewSkiplist(4)
	fmt.Println("inserting items:", n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			p := new(int)
			*p = rand.Intn(1000)
			s.InsertThreadSafe(RawPointer(p), (uint64)(i))
			atomic.AddUint64(&ops, 1)
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Println("inserted ops:", ops)
	s.Print()
}

func TestSkiplistInsertAtomicsAsyncBigNoGC(t *testing.T) {
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
			s.InsertThreadSafe(RawPointer(p), (uint64)(i))
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

func doInsert1(s *Skiplist, ptr RawPointer, wg *sync.WaitGroup, goid int) {
	s.InsertThreadSafe(ptr, (uint64)(goid))
	wg.Done()
}

func doInsert2(s *Skiplist, wg *sync.WaitGroup, n int, isRand bool) {
	defer wg.Done()
	var val int
	if isRand {
		rnd := rand.New(rand.NewSource(int64(rand.Int())))
		val = rnd.Int()
	} else {
		val = n
	}
	itm := int(val)
	s.InsertThreadSafe(unsafe.Pointer(&itm), (uint64)(1))
}
func TestSkiplistInsertGC1(t *testing.T) {
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
		go doInsert1(s, RawPointer(&p[i]), &wg, i)
	}
	wg.Wait()
	debug.ReadGCStats(&stats)
	//fmt.Println("GC (%):", debug.SetGCPercent(100))
	fmt.Println("num gc:", stats.NumGC, "gctime:", stats.PauseTotal)
	s.Print()
}

func TestSkiplistInsertGC2(t *testing.T) {
	var wg sync.WaitGroup
	var stats debug.GCStats
	n := 1000000
	fmt.Println("inserting items:", n)
	gc := debug.SetGCPercent(100)
	fmt.Println("GC (%):", gc)
	s := NewSkiplist(4)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go doInsert2(s, &wg, i, false)
	}
	wg.Wait()
	debug.ReadGCStats(&stats)
	//fmt.Println("GC (%):", debug.SetGCPercent(100))
	fmt.Println("num gc:", stats.NumGC, "gctime:", stats.PauseTotal)
	s.Print()
}

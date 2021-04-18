// Copyright (c) 2021 Saptarshi Sen

package goproj

import (
	"fmt"
	"testing"
)

func TestStorageEngine(t *testing.T) {
	engine := InitializeStorageEngine()
	for i := 0; i < 8; i++ {
		engine.InsertKey((int64)(i))
	}
	snap_v1 := engine.NewSnapShot()
	snap_v1.VisitItems()

	for i := 0; i < 4; i++ {
		engine.DeleteKey((int64)(i))
	}
	snap_v2 := engine.NewSnapShot()
	snap_v2.VisitItems()

	engine.DeleteKey((int64)(4))
	snap_v2.VisitItems()

	engine.VisitSnapshots()
	engine.VisitItems()
	engine.ShutdownGC()
}

func TestStorageEngineGC(t *testing.T) {
	engine := InitializeStorageEngine()
	for i := 0; i < 8; i++ {
		engine.InsertKey((int64)(i))
	}
	snap_v1 := engine.NewSnapShot()
	snap_v1.VisitItems()

	for i := 0; i < 4; i++ {
		engine.DeleteKey((int64)(i))
	}
	snap_v2 := engine.NewSnapShot()
	snap_v2.VisitItems()

	engine.DeleteKey((int64)(4))
	snap_v2.VisitItems()

	// 0->7
	engine.DeleteSnapshot(snap_v2)
	engine.VisitSnapshots()
	engine.VisitItems()

	// 4->7
	engine.DeleteSnapshot(snap_v1)
	engine.VisitSnapshots()
	engine.VisitItems()

	engine.ShutdownGC()
	// 4-7
	engine.VisitItems()
	ShutdownStorageEngine(engine)
}

func TestStorageEngineLoadStoreSnapshot(t *testing.T) {
	var v1 int64
	var c1, c2 uint64
	engine := InitializeStorageEngine()
	for i := 0; i < 8; i++ {
		engine.InsertKey((int64)(i))
	}
	snap_v1 := engine.NewSnapShot()
	v1 = snap_v1.version
	c1 = engine.VisitItems()
	engine.SaveSnapshot(snap_v1)
	v1 = snap_v1.version
	engine.ShutdownGC()
	ShutdownStorageEngine(engine)

	engine = InitializeStorageEngine()
	engine.LoadSnapshot(v1)
	c2 = engine.VisitItems()
	if c1 != c2 {
		panic("snapshot count mismatch!!!")
	}
	engine.ShutdownGC()
	ShutdownStorageEngine(engine)
}

func TestStorageEngineLoadStoreManySnapshots(t *testing.T) {
	var v1, v2 int64
	var c1, c2 uint64
	db_instance00 := InitializeStorageEngine()
	for i := 0; i < 8; i++ {
		db_instance00.InsertKey((int64)(i))
	}
	snap_v1 := db_instance00.NewSnapShot()
	c1 = 8
	v1 = snap_v1.version
	db_instance00.SaveSnapshot(snap_v1)

	for i := 4; i < 8; i++ {
		db_instance00.DeleteKey((int64)(i))
	}
	for i := 8; i < 12; i++ {
		db_instance00.InsertKey((int64)(i))
	}
	snap_v2 := db_instance00.NewSnapShot()
	c2 = 8
	v2 = snap_v2.version
	db_instance00.SaveSnapshot(snap_v2)
	db_instance00.ShutdownGC()
	ShutdownStorageEngine(db_instance00)

	db_instance01 := InitializeStorageEngine()
	db_instance01.LoadSnapshot(v1)
	fmt.Println("Priniting Snapshot Version:", v1)
	if c1 != db_instance01.VisitItems() {
		panic("snapshot item count mismatch")
	}
	db_instance01.ShutdownGC()
	ShutdownStorageEngine(db_instance01)

	db_instance02 := InitializeStorageEngine()
	db_instance02.LoadSnapshot(v2)
	fmt.Println("Priniting Snapshot Version:", v2)
	if c2 != db_instance02.VisitItems() {
		panic("snapshot item count mismatch")
	}
	db_instance02.ShutdownGC()
	ShutdownStorageEngine(db_instance02)
}

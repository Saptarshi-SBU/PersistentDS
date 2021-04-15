// Copyright (c) 2021 Saptarshi Sen

package goproj

import (
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
}

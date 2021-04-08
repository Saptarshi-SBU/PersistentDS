// Copyright (c) 2021 Saptarshi Sen

package goproj

import (
	"fmt"
	"testing"
	"unsafe"
)

func TestNodeMeta(t *testing.T) {
	var n Node
	fmt.Println(n.Size())
	fmt.Println(n.Level())
}

func TestNodeMaxMin(t *testing.T) {
	var (
		minItem = unsafe.Pointer(nil)
		maxItem = unsafe.Pointer(^uintptr(0))
	)
	fmt.Println(minItem)
	fmt.Println(maxItem)
}

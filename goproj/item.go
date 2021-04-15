// Copyright (c) 2021 Saptarshi Sen

package goproj

// our data
type Item struct {
	data      int64
	snap_meta ItemSnapMeta
}

type ItemSnapMeta struct {
	born_vers int64
	dead_vers int64
}

// allocates the entry
func AllocateItem(val int64, vers int64) *Item {
	return &Item{
		data:      val,
		snap_meta: ItemSnapMeta{born_vers: vers, dead_vers: 100},
	}
}

// fetch the stored data
func (item *Item) GetItemValue() int64 {
	return item.data
}

// fetch the associated metadata
func (item *Item) GetItemMetaInfo() ItemSnapMeta {
	return item.snap_meta
}

// delete the entry
func (item *Item) DeleteItem(curr_version int64) {
	item.snap_meta.dead_vers = curr_version
}

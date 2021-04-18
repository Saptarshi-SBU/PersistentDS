// Copyright (c) 2021 Saptarshi Sen

package goproj

const MaxInt64 int64 = int64(^uint(0) >> 1)

// our data
type Item struct {
	data      int64        `json:"data"`
	snap_meta ItemSnapMeta `json:"snap_meta"`
}

type ItemSnapMeta struct {
	born_vers int64 `json:"born_vers"`
	dead_vers int64 `json:"dead_vers"`
}

// allocates the entry
func AllocateItem(val int64, vers int64) *Item {
	return &Item{
		data:      val,
		snap_meta: ItemSnapMeta{born_vers: vers, dead_vers: MaxInt64},
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

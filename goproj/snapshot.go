// Copyright (c) 2021 Saptarshi Sen

package goproj

import "log"

type Snapshot struct {
	version  int64
	refcount int64
	i_list   *Skiplist
}

func (s *Snapshot) VisitItems() {
	log.Println("snapshot iterator scan for version:", s.version)
	it := s.i_list.NewIterator()
	for it.NextMutable(); it.isValid(); it.NextMutable() {
		item := (*Item)(it.GetData())
		meta := item.GetItemMetaInfo()
		//log.Println("visited key:", item.GetItemValue(), meta.born_vers, meta.dead_vers)
		if s.version >= meta.born_vers {
			if meta.dead_vers == 0 || (s.version < meta.dead_vers) {
				log.Println("visited key:", item.GetItemValue())
			}
		}
	}
}

// Copyright (C) 2019 rameshvk. All rights reserved.
// Use of this source code is governed by a MIT-style license
// that can be found in the LICENSE file.

package partition

import (
	"context"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

func NewHashRing() func(ctx context.Context, list []string, hash uint64) string {
	ring := &hashring{factor: 1000}
	return ring.Pick
}

type hashring struct {
	factor int // how many time to duplicate a single key for getting uniform spread

	sync.Mutex
	list   []string
	hashes []uint32          // sorted
	lookup map[uint32]string // hash => list entry
}

func (h *hashring) Pick(ctx context.Context, list []string, hash uint64) string {
	if len(list) == 0 {
		return ""
	}

	hash32 := crc32.ChecksumIEEE([]byte(fmt.Sprint(hash)))
	hashes, lookup := h.getSortedHashes(list)

	idx := sort.Search(len(hashes), func(i int) bool { return hashes[i] >= hash32 })
	if idx == len(hashes) {
		idx = 0
	}

	return lookup[hashes[idx]]
}

func (h *hashring) getSortedHashes(list []string) ([]uint32, map[uint32]string) {
	h.Lock()
	defer h.Unlock()

	if h.listsDifferent(h.list, list) {
		h.list = list
		h.hashes, h.lookup = h.calculateSortedHashes(list)
	}
	return h.hashes, h.lookup
}

func (h *hashring) calculateSortedHashes(list []string) ([]uint32, map[uint32]string) {
	hashes := make([]uint32, len(list)*h.factor)
	dict := map[uint32]string{}
	for kk := range list {
		for ff := 0; ff < h.factor; ff++ {
			data := []byte(strconv.Itoa(ff*12394+1) + "-" + list[kk])
			idx := kk*h.factor + ff
			hashes[idx] = crc32.ChecksumIEEE(data)
			dict[hashes[idx]] = list[kk]
		}
	}
	sort.Slice(hashes, func(i, j int) bool { return hashes[i] < hashes[j] })
	return hashes, dict
}

func (h *hashring) listsDifferent(l1, l2 []string) bool {
	if len(l1) != len(l2) {
		return true
	}
	for kk := range l1 {
		if l1[kk] != l2[kk] {
			return true
		}
	}

	return false
}

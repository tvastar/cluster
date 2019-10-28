// Copyright (C) 2019 rameshvk. All rights reserved.
// Use of this source code is governed by a MIT-style license
// that can be found in the LICENSE file.

package partition

import (
	"context"
	"hash/crc32"
	"strconv"
)

// NewPicker returns a picker which uses a highest random weight algorithm
//
// See https://en.wikipedia.org/wiki/Rendezvous_hashing
func NewPicker() func(ctx context.Context, list []string, hash uint64) string {
	return hrwPicker
}

func hrwPicker(ctx context.Context, list []string, hash uint64) string {
	var pick string
	var score uint32

	h := crc32.NewIEEE()
	for _, candidate := range list {
		checkWrite(h.Write([]byte(strconv.FormatUint(hash, 16))))
		checkWrite(h.Write([]byte(candidate)))
		if x := h.Sum32(); x > score {
			pick, score = candidate, x
		}
		h.Reset()
	}
	return pick
}

func checkWrite(n int, err error) {
	if err != nil {
		panic(err)
	}
}

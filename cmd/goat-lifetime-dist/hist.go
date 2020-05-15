// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

type SmallUint32Hist struct {
	bins []uint64
}

func (h *SmallUint32Hist) AddN(i uint32, n uint64) {
	if i >= uint32(len(h.bins)) {
		h.bins = append(h.bins, make([]uint64, i-uint32(len(h.bins))+1)...)
	}
	h.bins[i] += n
}

func (h *SmallUint32Hist) Add(i uint32) {
	h.AddN(i, 1)
}

func (h *SmallUint32Hist) Snapshot() []uint64 {
	out := make([]uint64, 0, len(h.bins))
	for i := range h.bins {
		out = append(out, h.bins[i])
	}
	return out
}

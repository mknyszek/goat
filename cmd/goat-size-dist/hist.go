// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

type SizeHist struct {
	small [32 << 10]uint64
	large map[uint64]uint64
}

func NewSizeHist() *SizeHist {
	return &SizeHist{
		large: make(map[uint64]uint64),
	}
}

func (s *SizeHist) Add(size uint64) {
	if size <= uint64(len(s.small)) {
		s.small[size-1] += 1
		return
	}
	if val, ok := s.large[size]; ok {
		s.large[size] = val + 1
	} else {
		s.large[size] = 1
	}
}

func (s *SizeHist) Sub(size uint64) {
	if size <= uint64(len(s.small)) {
		if s.small[size-1] == 0 {
			panic("subtraction below zero")
		}
		s.small[size-1] -= 1
		return
	}
	if val, ok := s.large[size]; ok {
		if val == 1 {
			delete(s.large, size)
		} else {
			s.large[size] = val - 1
		}
	} else {
		panic("subtraction below zero")
	}
}

func (s *SizeHist) ForEach(f func(size, count uint64)) {
	for i := range s.small {
		if s.small[i] != 0 {
			f(uint64(i+1), s.small[i])
		}
	}
	for size, count := range s.large {
		if count != 0 {
			f(size, count)
		}
	}
}

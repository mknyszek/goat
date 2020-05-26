// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goat

import (
	"errors"
	"fmt"
	"io"
	"math/bits"
	"runtime"
	"sort"
	"sync"

	"golang.org/x/sync/errgroup"
)

const batchSize = 32 << 10

// Parser contains the Go allocation trace parsing
// state.
type Parser struct {
	src          Source
	index        [][]batchOffset
	batches      []batchReader
	totalBatches uint64
}

// Source is an allocation trace source.
type Source interface {
	io.ReaderAt

	// Len returns the size of the allocation
	// trace in bytes.
	Len() int
}

type batchOffset struct {
	startTicks uint64
	fileOffset int64
}

func (b batchOffset) headerSize() uint64 {
	return 3 + uint64(bits.Len64(b.startTicks)+6)/7
}

const (
	atEvBad uint8 = iota
	atEvSpanAcquire
	atEvAlloc
	atEvAllocArray
	atEvAllocLarge
	atEvAllocLargeNoscan
	atEvAllocLargeArray
	atEvAllocLargeArrayNoscan
	atEvSpanRelease
	atEvSweep
	atEvFree
	atEvSweepTerm
	atEvMarkTerm
	atEvSync
	atEvBatchStart
	atEvBatchEnd
	atEvStackAlloc
	atEvStackFree
)

func parseVarint(buf []byte) (int, uint64, error) {
	result := uint64(0)
	shift := uint(0)
	i := 0
loop:
	if i >= len(buf) {
		return 0, 0, fmt.Errorf("not enough bytes left to decode varint")
	}
	result |= uint64(buf[i]&0x7f) << shift
	if buf[i]&(1<<7) == 0 {
		return i + 1, result, nil
	}
	shift += 7
	i++
	if shift >= 64 {
		return 0, 0, fmt.Errorf("varint too long")
	}
	goto loop
}

func parseBatchHeader(buf []byte) (int32, uint64, error) {
	idx := 0
	if buf[idx] != atEvBatchStart {
		return 0, 0, fmt.Errorf("expected batch start event")
	}
	idx++

	n, pid, err := parseVarint(buf[idx:])
	if err != nil {
		return 0, 0, err
	}
	idx += n

	if buf[idx] != atEvSync {
		return 0, 0, fmt.Errorf("expected sync event")
	}
	idx++

	_, ticks, err := parseVarint(buf[idx:])
	if err != nil {
		return 0, 0, err
	}
	return int32(pid), ticks, nil
}

const headerSize = 4

const supportedVersion uint16 = (uint16(1) << 8) | 15

func parseHeader(r Source) (uint16, error) {
	var header [headerSize]byte
	n, err := r.ReadAt(header[:], 0)
	if n != 4 || err != nil {
		return 0, err
	}
	version := uint16(header[2])<<8 | uint16(header[3])
	return version, nil
}

// NewParser creates and initializes new Parser given a Source.
//
// Initialization may involve ordering the trace, which may be
// computationally expensive.
//
// NewParser may fail if initialization, which may involve parsing
// part of or all of the trace, fails.
func NewParser(r Source) (*Parser, error) {
	// Check some basic properties, like the size and the header.
	if r.Len()%batchSize != headerSize {
		return nil, fmt.Errorf("bad format: file must be a multiple of %d bytes", batchSize)
	}
	version, err := parseHeader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to parse header: %v", err)
	}
	if version != supportedVersion {
		return nil, fmt.Errorf("unsupported version")
	}

	// Figure out how to break up the initialization phase.
	shards := runtime.GOMAXPROCS(-1)
	numBatches := (r.Len() - headerSize) / batchSize
	if shards > numBatches {
		shards = 1
	}
	batchesPerShard := numBatches / shards
	if numBatches%shards != 0 {
		batchesPerShard = numBatches / (shards - 1)
	}

	// Build up a per-shard index.
	perShardIndex := make([][][]batchOffset, shards)
	var eg errgroup.Group
	for i := 0; i < shards; i++ {
		i := i
		eg.Go(func() error {
			const bufSize = 16
			var buf [bufSize]byte

			// Generate the index for this shard.
			index := make([][]batchOffset, 16)
			start := int64(batchesPerShard * i)
			end := int64(batchesPerShard * (i + 1))
			if end > int64(numBatches) {
				end = int64(numBatches)
			}
			for idx := start*batchSize + headerSize; idx < end*batchSize+headerSize; idx += batchSize {
				n, err := r.ReadAt(buf[:], idx)
				if n < bufSize {
					return err
				}
				pid, ticks, err := parseBatchHeader(buf[:])
				if err != nil {
					return err
				}
				if int(pid) >= len(index) {
					index = append(index, make([][]batchOffset, int(pid)-len(index)+1)...)
				}
				index[pid] = append(index[pid], batchOffset{
					startTicks: ticks,
					fileOffset: idx,
				})
			}
			// For each P, sort the batches in the index.
			for pid := range index {
				sort.Slice(index[pid], func(i, j int) bool {
					return index[pid][i].startTicks < index[pid][j].startTicks
				})
			}
			perShardIndex[i] = index
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// Count the maximum number of Ps we need to account for.
	// Note that this may be more than the number of Ps actually
	// represented in the trace.
	maxP := 0
	for i := range perShardIndex {
		if ps := len(perShardIndex[i]); ps > maxP {
			maxP = ps
		}
	}

	// Count up how many batches there are for each P.
	perPidBatches := make([]int, maxP)
	for pid := range perPidBatches {
		for i := 0; i < shards; i++ {
			if pid < len(perShardIndex[i]) {
				perPidBatches[pid] += len(perShardIndex[i][pid])
			}
		}
	}

	// Merge the per-shard indicies into one index, parallelizing
	// across Ps.
	index := make([][]batchOffset, maxP)
	pidChan := make(chan int, shards)
	var wg sync.WaitGroup
	for i := 0; i < shards; i++ {
		go func() {
			for {
				pid, ok := <-pidChan
				if !ok {
					return
				}
				for len(index[pid]) < perPidBatches[pid] {
					minBatch := batchOffset{startTicks: ^uint64(0)}
					minShard := -1
					for i := 0; i < shards; i++ {
						if pid < len(perShardIndex[i]) && len(perShardIndex[i][pid]) > 0 && perShardIndex[i][pid][0].startTicks < minBatch.startTicks {
							minBatch = perShardIndex[i][pid][0]
							minShard = i
						}
					}
					perShardIndex[minShard][pid] = perShardIndex[minShard][pid][1:]
					index[pid] = append(index[pid], minBatch)
				}
				wg.Done()
			}
		}()
	}
	for pid := range index {
		if perPidBatches[pid] != 0 {
			wg.Add(1)
			pidChan <- pid
		}
	}
	wg.Wait()
	close(pidChan)

	p := &Parser{
		src:          r,
		index:        index,
		batches:      make([]batchReader, maxP),
		totalBatches: uint64(r.Len()-headerSize) / batchSize,
	}
	for pid := range index {
		if _, err := p.next(pid); err != nil {
			return nil, fmt.Errorf("initializing parser: %v", err)
		}
	}
	return p, nil
}

var doneEvent = Event{Timestamp: ^uint64(0)}
var streamEnd = errors.New("stream end")

type batchReader struct {
	next       Event
	syncTick   uint64
	allocBase  [^uint8(0)]uint64
	freeBase   uint64
	sweepStart uint64
	readBuf    []byte
	batchBuf   [batchSize]byte
}

func (b *batchReader) nextEvent() error {
	if len(b.readBuf) == 0 {
		return streamEnd
	}
	haveEvent := false
	b.next = Event{}
	for !haveEvent {
		size := 1
		switch evKind := b.readBuf[0]; evKind {
		case atEvSpanAcquire:
			// Parse class.
			class := b.readBuf[size]
			size += 1

			// Parse base address.
			n, base, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing span base: %v", err)
			}
			size += n
			b.allocBase[class] = base
		case atEvAllocArray:
			b.next.Array = true
			fallthrough
		case atEvAlloc:
			haveEvent = true
			b.next.Kind = EventAlloc

			// Parse class for alloc event.
			class := b.readBuf[size]
			size += 1

			// Parse offset for alloc event.
			n, allocOffset, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing offset for alloc: %v", err)
			}
			size += n

			// Parse size for alloc event.
			n, allocSizeDiff, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing size for alloc: %v", err)
			}
			size += n

			// Parse tick delta for alloc event.
			n, tickDelta, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing tick delta for alloc: %v", err)
			}
			size += n

			if class >= 2 && b.allocBase[class] == 0 {
				return fmt.Errorf("allocation from unacquired span class %d", class)
			}
			b.next.Timestamp = b.syncTick + tickDelta
			b.next.Address = b.allocBase[class] + allocOffset
			b.next.Size = classToSize(class) - allocSizeDiff
		case atEvAllocLargeArrayNoscan:
			fallthrough
		case atEvAllocLargeArray:
			b.next.Array = true
			fallthrough
		case atEvAllocLargeNoscan:
			fallthrough
		case atEvAllocLarge:
			haveEvent = true
			b.next.Kind = EventAlloc

			// Parse address for alloc event.
			n, addr, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing address for large alloc: %v", err)
			}
			size += n

			// Parse size for alloc event.
			n, allocSize, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing size for large alloc: %v", err)
			}
			size += n

			// Parse tick delta for alloc event.
			n, tickDelta, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing tick delta for alloc: %v", err)
			}
			size += n

			b.next.Timestamp = b.syncTick + tickDelta
			b.next.Address = addr
			b.next.Size = allocSize
		case atEvSpanRelease:
			// Parse class.
			class := b.readBuf[size]
			size += 1

			if b.allocBase[class] == 0 {
				return fmt.Errorf("release of unacquired span class")
			}
			b.allocBase[class] = 0
		case atEvSweep:
			// Parse tick delta for sweep event.
			n, tickDelta, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing ticks for sweep: %v", err)
			}
			size += n
			b.sweepStart = b.syncTick + tickDelta

			// Parse base address for sweep event.
			n, base, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing tick delta for sweep: %v", err)
			}
			size += n
			b.freeBase = base
		case atEvFree:
			haveEvent = true
			b.next.Kind = EventFree

			// Parse offset for free event.
			n, freeOffset, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing offset for free: %v", err)
			}
			size += n

			b.next.Timestamp = b.sweepStart
			b.next.Address = b.freeBase + freeOffset
		case atEvSweepTerm:
			haveEvent = true
			b.next.Kind = EventGCStart

			n, tickDelta, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing sweep termination event timestamp: %v", err)
			}
			size += n

			b.next.Timestamp = b.syncTick + tickDelta
		case atEvMarkTerm:
			haveEvent = true
			b.next.Kind = EventGCEnd

			n, tickDelta, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing mark termination event timestamp: %v", err)
			}
			size += n

			b.next.Timestamp = b.syncTick + tickDelta
		case atEvSync:
			n, ticks, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing sync event timestamp: %v", err)
			}
			size += n
			b.syncTick = ticks
		case atEvBatchEnd:
			return streamEnd
		case atEvBatchStart:
			return fmt.Errorf("unexpected header found")
		case atEvStackAlloc:
			haveEvent = true
			b.next.Kind = EventStackAlloc

			// Parse stack order.
			order := b.readBuf[size]
			size += 1

			// Parse stack base (stack.lo).
			n, base, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing mark termination event timestamp: %v", err)
			}
			size += n

			n, tickDelta, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing mark termination event timestamp: %v", err)
			}
			size += n

			b.next.Timestamp = b.syncTick + tickDelta
			b.next.Address = base
			b.next.Size = uint64(1 << order)
		case atEvStackFree:
			haveEvent = true
			b.next.Kind = EventStackFree

			// Parse stack base (stack.lo).
			n, base, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing mark termination event timestamp: %v", err)
			}
			size += n

			n, tickDelta, err := parseVarint(b.readBuf[size:])
			if err != nil {
				return fmt.Errorf("parsing mark termination event timestamp: %v", err)
			}
			size += n

			b.next.Timestamp = b.syncTick + tickDelta
			b.next.Address = base
		default:
			return fmt.Errorf("unknown event type %d", evKind)
		}
		b.readBuf = b.readBuf[size:]
	}
	return nil
}

func (p *Parser) peek(pid int) uint64 {
	return p.batches[pid].next.Timestamp
}

func (p *Parser) refill(pid int) error {
	// If we're out of batches, just mark
	// this P as done.
	if len(p.index[pid]) == 0 {
		p.batches[pid].next = doneEvent
		return nil
	}
	// Grab the next batch for this P.
	bo := p.index[pid][0]
	p.index[pid] = p.index[pid][1:]

	// Read in the batch.
	br := &p.batches[pid]
	n, err := p.src.ReadAt(br.batchBuf[:], bo.fileOffset)
	if n != len(br.batchBuf) {
		return err
	}

	// Skip the header.
	br.readBuf = br.batchBuf[bo.headerSize():]

	// Set the sync event tick for this batch,
	// which was present in the header.
	br.syncTick = bo.startTicks

	// Read the next event.
	if err := br.nextEvent(); err != nil && err != streamEnd {
		return fmt.Errorf("refill: P %d: %v", pid, err)
	}
	return nil
}

func (p *Parser) next(pid int) (Event, error) {
	// Grab the current event first.
	ev := p.batches[pid].next
	ev.P = int32(pid) - 1

	// Get the next event.
	if err := p.batches[pid].nextEvent(); err != nil && err != streamEnd {
		return Event{}, fmt.Errorf("P %d: %v", pid, err)
	} else if err == streamEnd {
		// We've run out of things to parse for this P! Refill.
		if err := p.refill(pid); err != nil {
			return Event{}, err
		}
	}
	return ev, nil
}

// Progress returns a float64 value between 0 and 1 indicating the
// approximate progress of parsing through the file.
func (p *Parser) Progress() float64 {
	left := uint64(0)
	for _, perPBatches := range p.index {
		left += uint64(len(perPBatches))
	}
	return float64(p.totalBatches-left) / float64(p.totalBatches)
}

// Next returns the next event in the trace, or an error
// if the parser failed to parse the next event out of the trace.
func (p *Parser) Next() (Event, error) {
	// Compute which P has the next event.
	minPid := -1
	minTick := ^uint64(0)
	for pid := range p.batches {
		if t := p.peek(pid); t < minTick {
			minTick = t
			minPid = pid
		}
	}

	// If there's no such event, signal that we're done.
	if minPid < 0 {
		return Event{}, io.EOF
	}

	// Return the event, and compute the next.
	return p.next(minPid)
}

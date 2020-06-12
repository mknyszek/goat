package object

import (
	"github.com/mknyszek/goat/simulation"
	"github.com/mknyszek/goat/simulation/toolbox"
)

type immixSpanList struct {
	first, last *immixSpan
}

func (l *immixSpanList) empty() bool {
	return l.first == nil
}

func (l *immixSpanList) pushFront(s *immixSpan) {
	if s.currList != nil {
		panic("stack span already on list")
	}
	s.next = l.first
	if l.first == nil {
		l.last = s
	} else {
		l.first.prev = s
	}
	l.first = s
	s.currList = l
}

func (l *immixSpanList) pushBack(s *immixSpan) {
	if s.currList != nil {
		panic("stack span already on list")
	}
	s.prev = l.last
	if l.first == nil {
		l.first = s
	} else {
		l.last.next = s
	}
	l.last = s
	s.currList = l
}

func (l *immixSpanList) remove(s *immixSpan) {
	if s.currList != l {
		panic("removing stack span from wrong list")
	}
	if l.first == s && l.last == s {
		l.first, l.last = nil, nil
	} else if l.first == s {
		s.next.prev = nil
		l.first = s.next
		s.next = nil
	} else if l.last == s {
		s.prev.next = nil
		l.last = s.prev
		s.prev = nil
	} else {
		s.prev.next = s.next
		s.next.prev = s.prev
		s.next = nil
		s.prev = nil
	}
	s.currList = nil
}

type immixSpanClass uint8

const (
	immixLarge immixSpanClass = iota
	immixTiny
	immixSmall
	immixMedium
	immixNumSpanClasses
)

const immixTinyMaxSize = 128

var immixClassToPages = [immixNumSpanClasses]toolbox.Pages{0, 1, 1, 16}

var immixClassToLineSize = [immixNumSpanClasses]toolbox.Bytes{0, 128, 256, 4096}

type immixSpan struct {
	currList     *immixSpanList
	next, prev   *immixSpan
	base         toolbox.Address
	npages       toolbox.Pages
	class        immixSpanClass
	cached       bool
	lineFreeIdx  uint64
	lineCount    uint64
	lineSize     toolbox.Bytes
	lineRefCount [64]uint16
	lineRefDec   [64]uint16
	bumpLo       toolbox.Address
	bumpHi       toolbox.Address
	allocCount   uint64
	freedCount   uint64
	unused       [64]toolbox.Bytes
}

func (s *immixSpan) alloc(ctx toolbox.Context, headerSize, size toolbox.Bytes) toolbox.Address {
	if s == nil {
		return 0
	}
	var align toolbox.Bytes
	switch s.class {
	case immixTiny:
		if size >= 8 {
			align = 8
		} else if size >= 4 {
			align = 4
		} else if size >= 2 {
			align = 2
		} else {
			align = 1
		}
	case immixSmall:
		align = 8
	case immixMedium:
		align = 128
	}
	lo := s.bumpLo.AlignUp(align)
	if lo.Add(size) <= s.bumpHi {
		unused := lo.Add(headerSize).Diff(s.bumpLo)
		{
			ctx.Stats.AddOther(immixHeaderStat, uint64(headerSize))
			ctx.Stats.FreeBytes -= uint64(unused)
			ctx.Stats.UnusedBytes += uint64(unused)
			if s.class == immixTiny {
				ctx.Stats.AddOther(immixTinyWasteStat, uint64(unused))
			} else if s.class == immixSmall {
				ctx.Stats.AddOther(immixSmallWasteStat, uint64(unused))
			} else if s.class == immixMedium {
				ctx.Stats.AddOther(immixMediumWasteStat, uint64(unused))
			}
			startLine := uint64(s.bumpLo.Diff(s.base) / s.lineSize)
			endLine := uint64(lo.Add(headerSize).Diff(s.base) / s.lineSize)
			total := toolbox.Bytes(0)
			nlo := s.bumpLo
			for i := startLine; i <= endLine; i++ {
				nnlo := (nlo + 1).AlignUp(s.lineSize)
				if i == endLine {
					nnlo = lo.Add(headerSize)
				}
				if nnlo.Diff(nlo) != 0 {
					total += nnlo.Diff(nlo)
					s.unused[i] += nnlo.Diff(nlo)
				}
				nlo = nnlo
			}
			if total != unused {
				panic("bad accounting")
			}
			ctx.Stats.FreeBytes -= uint64(size - headerSize)
			ctx.Stats.ObjectBytes += uint64(size - headerSize)
			ctx.Stats.Allocs++
		}
		// Update reference counts.
		startLine := uint64(lo.Diff(s.base) / s.lineSize)
		endLine := uint64((lo.Diff(s.base) + size - 1) / s.lineSize)
		for i := startLine; i <= endLine; i++ {
			if s.lineRefCount[i] == 0 {
				ctx.Stats.AddOther(immixLinesStat, 1)
			}
			s.lineRefCount[i]++
		}
		s.allocCount++
		s.bumpLo = lo.Add(size)
		return lo
	}
	return 0
}

func (s *immixSpan) oneOrMoreLineRemains() bool {
	return s != nil && s.bumpHi.Diff(s.bumpLo) >= s.lineSize
}

func (s *immixSpan) refill(ctx toolbox.Context) {
	if s == nil {
		return
	}

	unused := s.bumpHi.Diff(s.bumpLo)
	if unused != 0 {
		ctx.Stats.FreeBytes -= uint64(unused)
		ctx.Stats.UnusedBytes += uint64(unused)
		if s.class == immixTiny {
			ctx.Stats.AddOther(immixTinyWasteStat, uint64(unused))
		} else if s.class == immixSmall {
			ctx.Stats.AddOther(immixSmallWasteStat, uint64(unused))
		} else if s.class == immixMedium {
			ctx.Stats.AddOther(immixMediumWasteStat, uint64(unused))
		}
		startLine := uint64(s.bumpLo.Diff(s.base) / s.lineSize)
		endLine := uint64((s.bumpHi.Diff(s.base)) / s.lineSize)
		total := toolbox.Bytes(0)
		lo := s.bumpLo
		for i := startLine; i <= endLine; i++ {
			nlo := (lo + 1).AlignUp(s.lineSize)
			if i == endLine {
				nlo = s.bumpHi
			}
			if nlo.Diff(lo) != 0 {
				total += nlo.Diff(lo)
				s.unused[i] += nlo.Diff(lo)
			}
			lo = nlo
		}
		if total != unused {
			panic("bad accounting")
		}
	}

	start := uint64(0)
	size := uint64(0)
	for i := s.lineFreeIdx; i < s.lineCount; i++ {
		if s.lineRefCount[i] == 0 {
			if start == 0 {
				start = i
			}
			size++
		} else if size > 0 && s.lineRefCount[i] != 0 {
			break
		}
	}
	if size == 0 {
		s.lineFreeIdx = s.lineCount
		s.bumpLo = 0
		s.bumpHi = 0
		return
	}
	s.lineFreeIdx = start + size
	s.bumpLo = s.base.Add(toolbox.Bytes(start) * s.lineSize)
	s.bumpHi = s.base.Add(toolbox.Bytes(start+size) * s.lineSize)
}

func (s *immixSpan) sweep(ctx toolbox.Context) {
	s.allocCount -= s.freedCount
	ctx.Stats.Frees += s.freedCount
	s.freedCount = 0
	start := s.lineCount
	for i := uint64(0); i < s.lineCount; i++ {
		s.lineRefCount[i] -= s.lineRefDec[i]
		if s.lineRefCount[i] == 0 {
			ctx.Stats.FreeBytes += uint64(s.unused[i])
			ctx.Stats.UnusedBytes -= uint64(s.unused[i])
			if s.lineRefDec[i] != 0 {
				ctx.Stats.SubOther(immixLinesStat, 1)
			}
			if s.class == immixTiny {
				ctx.Stats.SubOther(immixTinyWasteStat, uint64(s.unused[i]))
			} else if s.class == immixSmall {
				ctx.Stats.SubOther(immixSmallWasteStat, uint64(s.unused[i]))
			} else if s.class == immixMedium {
				ctx.Stats.SubOther(immixMediumWasteStat, uint64(s.unused[i]))
			}
			s.unused[i] = 0
			if i < start {
				start = i
			}
		}
		s.lineRefDec[i] = 0
	}
	size := uint64(0)
	for i := start; i < s.lineCount; i++ {
		if s.lineRefCount[i] != 0 {
			break
		}
		size++
	}
	if size == 0 {
		s.lineFreeIdx = s.lineCount
		s.bumpLo = 0
		s.bumpHi = 0
		return
	}
	s.lineFreeIdx = start + size
	s.bumpLo = s.base.Add(toolbox.Bytes(start) * s.lineSize)
	s.bumpHi = s.base.Add(toolbox.Bytes(start+size) * s.lineSize)
}

type immixCentral struct {
	class   immixSpanClass
	partial [2]immixSpanList
	full    [2]immixSpanList
}

type immixCache struct {
	alloc    [immixNumSpanClasses]*immixSpan
	overflow [immixNumSpanClasses]*immixSpan
}

type Immix struct {
	sweptIdx      uint
	pageAllocator toolbox.PageAllocator
	index         map[toolbox.Address]*immixSpan
	caches        map[toolbox.P]*immixCache
	central       [immixNumSpanClasses]immixCentral
	objectSizes   map[toolbox.Address]toolbox.Bytes
}

func NewImmix(pa toolbox.PageAllocator) *Immix {
	if pa.BytesPerPage() != 8192 {
		panic("page allocator must have 8 KiB pages")
	}
	return &Immix{
		pageAllocator: pa,
		index:         make(map[toolbox.Address]*immixSpan),
		caches:        make(map[toolbox.P]*immixCache),
		objectSizes:   make(map[toolbox.Address]toolbox.Bytes),
	}
}

const (
	immixHeaderStat      = "ImmixLiveObjectHeaderBytes"
	immixLinesStat       = "ImmixLinesOccupied"
	immixTinyWasteStat   = "ImmixTinyObjectUnusedBytes"
	immixSmallWasteStat  = "ImmixSmallObjectUnusedBytes"
	immixMediumWasteStat = "ImmixMediumObjectUnusedBytes"
)

func (g *Immix) RegisterStats(stats *simulation.Stats) {
	g.pageAllocator.RegisterStats(stats)
	stats.RegisterOther(immixHeaderStat)
	stats.RegisterOther(immixLinesStat)
	stats.RegisterOther(immixTinyWasteStat)
	stats.RegisterOther(immixSmallWasteStat)
	stats.RegisterOther(immixMediumWasteStat)
}

func (g *Immix) refill(ctx toolbox.Context, spc immixSpanClass, overflow bool) {
	var s *immixSpan
	if overflow {
		s = g.caches[ctx.P].overflow[spc]
	} else {
		s = g.caches[ctx.P].alloc[spc]
	}
	if s != nil {
		if s.lineFreeIdx < s.lineCount {
			panic("refilling not totally full span")
		}
		s.cached = false
		g.central[spc].full[g.sweptIdx].pushFront(s)
	}
	if overflow {
		goto fresh
	}
	if list := &g.central[spc].partial[g.sweptIdx]; !list.empty() {
		list.last.cached = true
		if overflow {
			g.caches[ctx.P].overflow[spc] = list.last
		} else {
			g.caches[ctx.P].alloc[spc] = list.last
		}
		list.remove(list.last)
		return
	}
	if list := &g.central[spc].partial[1-g.sweptIdx]; !list.empty() {
		list.last.cached = true
		if overflow {
			g.caches[ctx.P].overflow[spc] = list.last
		} else {
			g.caches[ctx.P].alloc[spc] = list.last
		}
		list.last.sweep(ctx)
		list.remove(list.last)
		return
	}
	for {
		if list := &g.central[spc].full[1-g.sweptIdx]; !list.empty() {
			s := list.last
			list.remove(s)
			s.sweep(ctx)
			if s.lineFreeIdx < s.lineCount {
				s.cached = true
				if overflow {
					g.caches[ctx.P].overflow[spc] = s
				} else {
					g.caches[ctx.P].alloc[spc] = s
				}
				return
			}
			g.central[spc].full[g.sweptIdx].pushFront(s)
		} else {
			break
		}
	}
fresh:
	// Create small object span.
	pageSize := g.pageAllocator.BytesPerPage()
	npages := immixClassToPages[spc]
	lineSize := immixClassToLineSize[spc]
	lineCount := uint64(npages.Bytes(pageSize) / lineSize)
	x := g.pageAllocator.AllocPages(ctx, npages)
	s = &immixSpan{
		class:       spc,
		base:        x,
		npages:      npages,
		lineSize:    lineSize,
		lineCount:   lineCount,
		lineFreeIdx: lineCount,
		bumpLo:      x,
		bumpHi:      x.Add(npages.Bytes(pageSize)),
	}
	if spc == immixTiny {
		// The first line is occupied by ptr-scan bits.
		s.bumpLo = x.Add(2 * s.lineSize)
		s.lineRefCount[0] = 1
		s.lineRefCount[1] = 1
		s.unused[0] = s.lineSize
		s.unused[1] = s.lineSize
		ctx.Stats.FreeBytes -= uint64(2 * s.lineSize)
		ctx.Stats.UnusedBytes += uint64(2 * s.lineSize)
		ctx.Stats.AddOther(immixTinyWasteStat, uint64(2*s.lineSize))
	}
	g.addToIndex(s)
	if overflow {
		g.caches[ctx.P].overflow[spc] = s
	} else {
		g.caches[ctx.P].alloc[spc] = s
	}
}

func (g *Immix) addToIndex(s *immixSpan) {
	for i := toolbox.Pages(0); i < s.npages; i++ {
		addr := s.base.Add(i.Bytes(g.pageAllocator.BytesPerPage()))
		g.index[addr] = s
	}
}

func (g *Immix) removeFromIndex(s *immixSpan) {
	for i := toolbox.Pages(0); i < s.npages; i++ {
		addr := s.base.Add(i.Bytes(g.pageAllocator.BytesPerPage()))
		delete(g.index, addr)
	}
}

func (g *Immix) AllocObject(ctx toolbox.Context, size toolbox.Bytes, array, _ bool) toolbox.Address {
	if ctx.P == toolbox.NoP {
		panic("allocation must be called with a P")
	}
	if size <= 32<<10 {
		headerSize := toolbox.Bytes(0)
		if size > immixTinyMaxSize {
			headerSize += 8
			if array && size > 464 {
				headerSize += 8
			}
		}
		dataSize := size
		size += headerSize
		c, ok := g.caches[ctx.P]
		if !ok {
			c = new(immixCache)
			g.caches[ctx.P] = c
		}
		spc := immixMedium
		if size <= 2<<10 {
			spc = immixSmall
		}
		if size <= immixTinyMaxSize {
			spc = immixTiny
		}
		var x toolbox.Address
		s := c.alloc[spc]
	loop:
		x = s.alloc(ctx, headerSize, size)
		if x == 0 {
			if s.oneOrMoreLineRemains() {
				x = c.overflow[spc].alloc(ctx, headerSize, size)
				if x == 0 {
					c.overflow[spc].refill(ctx)
					g.refill(ctx, spc, true)
					x = c.overflow[spc].alloc(ctx, headerSize, size)
				}
			} else if s != nil && s.lineFreeIdx < s.lineCount {
				s.refill(ctx)
				goto loop
			} else {
				if s != nil {
					s.refill(ctx)
				}
				g.refill(ctx, spc, false)
				s = c.alloc[spc]
				goto loop
			}
		}
		arrayBit := toolbox.Bytes(0)
		if headerSize > 8 {
			arrayBit = 1 << 1
		}
		g.objectSizes[x] = ((dataSize << 2) | 1) | arrayBit
		return x
	}
	pageSize := g.pageAllocator.BytesPerPage()
	npages := size.Pages(pageSize)
	x := g.pageAllocator.AllocPages(ctx, npages)
	s := &immixSpan{
		class:       immixLarge,
		base:        x,
		npages:      npages,
		lineFreeIdx: 1,
		lineCount:   1,
		lineSize:    size,
		allocCount:  1,
	}
	s.unused[0] = npages.Bytes(pageSize) - size
	s.lineRefCount[0] = 1
	ctx.Stats.AddOther(immixLinesStat, 1)
	g.central[immixLarge].full[g.sweptIdx].pushFront(s)
	g.addToIndex(s)
	g.objectSizes[x] = size << 2
	ctx.Stats.FreeBytes -= uint64(s.npages.Bytes(pageSize))
	ctx.Stats.ObjectBytes += uint64(size)
	ctx.Stats.UnusedBytes += uint64(s.unused[0])
	ctx.Stats.Allocs++
	return x
}

func (g *Immix) DeadObject(ctx toolbox.Context, addr toolbox.Address) {
	// Get the span, size of the object, and size of the object's header.
	s := g.index[addr.AlignDown(g.pageAllocator.BytesPerPage())]
	sizeVal := g.objectSizes[addr]
	delete(g.objectSizes, addr)
	headerSize := toolbox.Bytes(0)
	dataSize := sizeVal >> 2
	if dataSize > immixTinyMaxSize {
		if sizeVal&1 != 0 {
			headerSize += 8
		}
		if sizeVal&(1<<1) != 0 {
			headerSize += 8
		}
	}
	size := dataSize + headerSize

	// Mark the object's space as free.
	startLine := uint64(addr.Diff(s.base) / s.lineSize)
	endLine := uint64((addr.Diff(s.base) + size - 1) / s.lineSize)
	for i := startLine; i <= endLine; i++ {
		s.lineRefDec[i]++
	}
	s.freedCount++

	// No spans should be cached here.
	if s.cached {
		panic("found cached span at start of sweep")
	}

	// Mark the object's space as unused (it'll be cleaned
	// up on sweep).
	ctx.Stats.ObjectBytes -= uint64(dataSize)
	ctx.Stats.UnusedBytes += uint64(dataSize)
	if s.class == immixTiny {
		ctx.Stats.AddOther(immixTinyWasteStat, uint64(dataSize))
	} else if s.class == immixSmall {
		ctx.Stats.AddOther(immixSmallWasteStat, uint64(dataSize))
	} else if s.class == immixMedium {
		ctx.Stats.AddOther(immixMediumWasteStat, uint64(dataSize))
	}
	ctx.Stats.SubOther(immixHeaderStat, uint64(headerSize))
	if s.class != immixLarge {
		lo := addr.Add(headerSize)
		startLine = uint64(lo.Diff(s.base) / s.lineSize)
		endLine = uint64((lo.Diff(s.base) + dataSize - 1) / s.lineSize)
		total := toolbox.Bytes(0)
		for i := startLine; i <= endLine; i++ {
			nlo := (lo + 1).AlignUp(s.lineSize)
			if i == endLine {
				nlo = addr.Add(size)
			}
			total += nlo.Diff(lo)
			s.unused[i] += nlo.Diff(lo)
			lo = nlo
		}
		if total != dataSize {
			panic("bad accounting")
		}
	} else {
		s.unused[0] += dataSize
	}

	if s.freedCount == s.allocCount {
		// Sweep immediately.
		g.removeFromIndex(s)
		s.currList.remove(s)
		g.pageAllocator.FreePages(ctx, s.base, s.npages)
		for i := uint64(0); i < s.lineCount; i++ {
			ctx.Stats.FreeBytes += uint64(s.unused[i])
			ctx.Stats.UnusedBytes -= uint64(s.unused[i])
			if s.class == immixTiny {
				ctx.Stats.SubOther(immixTinyWasteStat, uint64(s.unused[i]))
			} else if s.class == immixSmall {
				ctx.Stats.SubOther(immixSmallWasteStat, uint64(s.unused[i]))
			} else if s.class == immixMedium {
				ctx.Stats.SubOther(immixMediumWasteStat, uint64(s.unused[i]))
			}
			if ((s.class == immixTiny && i > 1) || s.class != immixTiny) && s.lineRefCount[i] != 0 {
				ctx.Stats.SubOther(immixLinesStat, 1)
			}
			if ((s.class == immixTiny && i > 1) || s.class != immixTiny) && s.lineRefCount[i] != s.lineRefDec[i] {
				panic("totally free span doesn't have matching ref count and dec")
			}
		}
		ctx.Stats.Frees += s.freedCount
	}
}

func (g *Immix) GCStart(ctx toolbox.Context) {
	for spci := range g.central {
		spc := immixSpanClass(spci)
		partial := &g.central[spc].partial[1-g.sweptIdx]
		for !partial.empty() {
			s := partial.last
			partial.remove(s)
			s.sweep(ctx)
			g.central[spc].partial[g.sweptIdx].pushFront(s)
		}
		full := &g.central[spc].full[1-g.sweptIdx]
		for !full.empty() {
			s := full.last
			full.remove(s)
			s.sweep(ctx)
			if s.lineFreeIdx < s.lineCount {
				g.central[spc].partial[g.sweptIdx].pushFront(s)
			} else {
				g.central[spc].full[g.sweptIdx].pushFront(s)
			}
		}
	}
}

func (g *Immix) GCEnd(ctx toolbox.Context) {
	// Flush all caches for sweeping.
	for _, cache := range g.caches {
		for spc, s := range cache.alloc {
			if s != nil {
				s.cached = false
				if s.lineFreeIdx == s.lineCount {
					g.central[spc].full[g.sweptIdx].pushFront(s)
				} else {
					g.central[spc].partial[g.sweptIdx].pushFront(s)
				}
				cache.alloc[spc] = nil
			}
		}
		for spc, s := range cache.overflow {
			if s != nil {
				s.cached = false
				if s.lineFreeIdx == s.lineCount {
					g.central[spc].full[g.sweptIdx].pushFront(s)
				} else {
					g.central[spc].partial[g.sweptIdx].pushFront(s)
				}
				cache.overflow[spc] = nil
			}
		}
	}
	if g.sweptIdx != 0 {
		g.sweptIdx = 0
	} else {
		g.sweptIdx = 1
	}
}

package object

import (
	"github.com/mknyszek/goat/simulation"
	"github.com/mknyszek/goat/simulation/toolbox"
)

type go115SpanList struct {
	first, last *go115Span
}

func (l *go115SpanList) empty() bool {
	return l.first == nil
}

func (l *go115SpanList) pushFront(s *go115Span) {
	if s.currList != nil {
		panic("span already on list")
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

func (l *go115SpanList) pushBack(s *go115Span) {
	if s.currList != nil {
		panic("span already on list")
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

func (l *go115SpanList) remove(s *go115Span) {
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

type go115Span struct {
	currList   *go115SpanList
	next, prev *go115Span
	base       toolbox.Address
	npages     toolbox.Pages
	class      go115SpanClass
	elemSize   toolbox.Bytes
	numElems   uint64
	allocCount uint64
	freedCount uint64
	cached     bool
	free       [128]uint64
	freed      [128]uint64
	tailWaste  toolbox.Bytes
	objUnused  toolbox.Bytes
	scUnused   toolbox.Bytes
	scFreed    toolbox.Bytes
}

func (s *go115Span) allocObject() toolbox.Address {
	if s == nil {
		return 0
	}
	for i := uint64(0); i < s.numElems; i++ {
		if s.free[i/64]&(uint64(1)<<(i%64)) != 0 {
			s.free[i/64] &^= uint64(1) << (i % 64)
			s.allocCount++
			return s.base.Add(toolbox.Bytes(i) * s.elemSize)
		}
	}
	return 0
}

func (s *go115Span) sweep(ctx toolbox.Context) {
	for i := uint64(0); i < (s.numElems+63)/64; i++ {
		s.free[i] |= s.freed[i]
		s.freed[i] = 0
	}
	s.allocCount -= s.freedCount
	ctx.Stats.Frees += s.freedCount
	s.freedCount = 0
	s.scUnused -= s.scFreed
	ctx.Stats.SubOther(go115SCWasteStat, uint64(s.scFreed))
	ctx.Stats.SubOther(go115ObjectWasteStat, uint64(s.objUnused))
	ctx.Stats.FreeBytes += uint64(s.objUnused + s.scFreed)
	ctx.Stats.UnusedBytes -= uint64(s.objUnused + s.scFreed)
	s.objUnused = 0
	s.scFreed = 0
}

type go115Central struct {
	class   go115SpanClass
	partial [2]go115SpanList
	full    [2]go115SpanList
}

type go115Cache struct {
	alloc [go115NumSpanClasses]*go115Span
}

type Go115 struct {
	sweptIdx      uint
	pageAllocator toolbox.PageAllocator
	index         map[toolbox.Address]*go115Span
	caches        map[toolbox.P]*go115Cache
	central       [go115NumSpanClasses]go115Central
	objectSizes   map[toolbox.Address]toolbox.Bytes
}

func NewGo115(pa toolbox.PageAllocator) *Go115 {
	if pa.BytesPerPage() != 8192 {
		panic("page allocator must have 8 KiB pages")
	}
	g := &Go115{
		pageAllocator: pa,
		index:         make(map[toolbox.Address]*go115Span),
		caches:        make(map[toolbox.P]*go115Cache),
		objectSizes:   make(map[toolbox.Address]toolbox.Bytes),
	}
	return g
}

const (
	go115ObjectWasteStat = "Go115ObjectUnusedBytes"
	go115SCWasteStat     = "Go115ObjectTailUnusedBytes"
	go115TailWasteStat   = "Go115TailUnusedBytes"
)

func (g *Go115) RegisterStats(stats *simulation.Stats) {
	g.pageAllocator.RegisterStats(stats)
	stats.RegisterOther(go115ObjectWasteStat)
	stats.RegisterOther(go115SCWasteStat)
	stats.RegisterOther(go115TailWasteStat)
}

func (g *Go115) refill(ctx toolbox.Context, spc go115SpanClass) {
	s := g.caches[ctx.P].alloc[spc]
	if s != nil {
		if s.allocCount != s.numElems {
			panic("refilling not totally full span")
		}
		s.cached = false
		g.central[spc].full[g.sweptIdx].pushFront(s)
	}
	if list := &g.central[spc].partial[g.sweptIdx]; !list.empty() {
		list.last.cached = true
		g.caches[ctx.P].alloc[spc] = list.last
		list.remove(list.last)
		return
	}
	if list := &g.central[spc].partial[1-g.sweptIdx]; !list.empty() {
		list.last.cached = true
		g.caches[ctx.P].alloc[spc] = list.last
		list.last.sweep(ctx)
		list.remove(list.last)
		return
	}
	for {
		if list := &g.central[spc].full[1-g.sweptIdx]; !list.empty() {
			s := list.last
			s.sweep(ctx)
			list.remove(s)
			if s.allocCount < s.numElems {
				s.cached = true
				g.caches[ctx.P].alloc[spc] = s
				return
			}
			g.central[spc].full[g.sweptIdx].pushFront(s)
		} else {
			break
		}
	}
	pageSize := g.pageAllocator.BytesPerPage()
	npages := go115ClassToPages[spc.sizeClass()]
	elemSize := go115SizeClassToSize[spc.sizeClass()]
	numElems := uint64(npages.Bytes(pageSize) / elemSize)
	x := g.pageAllocator.AllocPages(ctx, npages)
	s = &go115Span{
		class:      spc,
		base:       x,
		npages:     npages,
		elemSize:   elemSize,
		numElems:   numElems,
		allocCount: 0,
		tailWaste:  npages.Bytes(pageSize) - elemSize*toolbox.Bytes(numElems),
		cached:     true,
	}
	for i := uint64(0); i < s.numElems; i++ {
		s.free[i/64] |= uint64(1) << (i % 64)
	}
	ctx.Stats.FreeBytes -= uint64(s.tailWaste)
	ctx.Stats.UnusedBytes += uint64(s.tailWaste)
	ctx.Stats.AddOther(go115TailWasteStat, uint64(s.tailWaste))
	g.addToIndex(s)
	g.caches[ctx.P].alloc[spc] = s
}

func (g *Go115) addToIndex(s *go115Span) {
	for i := toolbox.Pages(0); i < s.npages; i++ {
		addr := s.base.Add(i.Bytes(g.pageAllocator.BytesPerPage()))
		g.index[addr] = s
	}
}

func (g *Go115) removeFromIndex(s *go115Span) {
	for i := toolbox.Pages(0); i < s.npages; i++ {
		addr := s.base.Add(i.Bytes(g.pageAllocator.BytesPerPage()))
		delete(g.index, addr)
	}
}

func (g *Go115) AllocObject(ctx toolbox.Context, size toolbox.Bytes, _, noscan bool) toolbox.Address {
	if ctx.P == toolbox.NoP {
		panic("allocation must be called with a P")
	}
	if size <= go115MaxSmallObjectSize {
		c, ok := g.caches[ctx.P]
		if !ok {
			c = new(go115Cache)
			g.caches[ctx.P] = c
		}
		spc := makeGo115SpanClass(go115SizeToClass(size), noscan)
		x := c.alloc[spc].allocObject()
		if x == 0 {
			g.refill(ctx, spc)
			x = c.alloc[spc].allocObject()
		}
		g.objectSizes[x] = size
		diff := c.alloc[spc].elemSize - size
		c.alloc[spc].scUnused += diff
		ctx.Stats.AddOther(go115SCWasteStat, uint64(diff))
		ctx.Stats.FreeBytes -= uint64(c.alloc[spc].elemSize)
		ctx.Stats.ObjectBytes += uint64(size)
		ctx.Stats.UnusedBytes += uint64(diff)
		ctx.Stats.Allocs++
		return x
	}
	pageSize := g.pageAllocator.BytesPerPage()
	npages := size.Pages(pageSize)
	spc := makeGo115SpanClass(0, noscan)
	x := g.pageAllocator.AllocPages(ctx, npages)
	s := &go115Span{
		class:      spc,
		base:       x,
		npages:     npages,
		elemSize:   size,
		numElems:   1,
		allocCount: 1,
		tailWaste:  npages.Bytes(pageSize) - size,
	}
	s.free[0] = 1
	g.central[spc].full[g.sweptIdx].pushFront(s)
	g.addToIndex(s)
	g.objectSizes[x] = size
	ctx.Stats.FreeBytes -= uint64(s.npages.Bytes(pageSize))
	ctx.Stats.ObjectBytes += uint64(s.elemSize)
	ctx.Stats.UnusedBytes += uint64(s.tailWaste)
	ctx.Stats.AddOther(go115TailWasteStat, uint64(s.tailWaste))
	ctx.Stats.Allocs++
	return x
}

func (g *Go115) DeadObject(ctx toolbox.Context, addr toolbox.Address) {
	a := addr.AlignDown(g.pageAllocator.BytesPerPage())
	s := g.index[a]
	size := g.objectSizes[addr]
	delete(g.objectSizes, addr)
	objIndex := uint64(s.base.Diff(addr) / s.elemSize)
	s.freed[objIndex/64] |= uint64(1) << (objIndex % 64)
	s.freedCount++
	if s.cached {
		panic("found cached span at start of sweep")
	}
	ctx.Stats.ObjectBytes -= uint64(size)
	ctx.Stats.UnusedBytes += uint64(size)
	ctx.Stats.AddOther(go115ObjectWasteStat, uint64(size))
	s.objUnused += size
	s.scFreed += s.elemSize - size
	if s.freedCount == s.allocCount {
		g.removeFromIndex(s)
		s.currList.remove(s)
		g.pageAllocator.FreePages(ctx, s.base, s.npages)
		ctx.Stats.FreeBytes += uint64(s.tailWaste + s.objUnused + s.scUnused)
		ctx.Stats.UnusedBytes -= uint64(s.tailWaste + s.objUnused + s.scUnused)
		ctx.Stats.SubOther(go115TailWasteStat, uint64(s.tailWaste))
		ctx.Stats.SubOther(go115SCWasteStat, uint64(s.scUnused))
		ctx.Stats.SubOther(go115ObjectWasteStat, uint64(s.objUnused))
		ctx.Stats.Frees += s.freedCount
	}
}

func (g *Go115) GCStart(ctx toolbox.Context) {
	for spci := range g.central {
		spc := go115SpanClass(spci)
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
			if s.allocCount < s.numElems {
				g.central[spc].partial[g.sweptIdx].pushFront(s)
			} else {
				g.central[spc].full[g.sweptIdx].pushFront(s)
			}
		}
	}
}

func (g *Go115) GCEnd(ctx toolbox.Context) {
	// Flush all caches for sweeping.
	for _, cache := range g.caches {
		for spc, s := range cache.alloc {
			if s != nil {
				s.cached = false
				if s.allocCount == s.numElems {
					g.central[spc].full[g.sweptIdx].pushFront(s)
				} else {
					g.central[spc].partial[g.sweptIdx].pushFront(s)
				}
				cache.alloc[spc] = nil
			}
		}
	}
	if g.sweptIdx != 0 {
		g.sweptIdx = 0
	} else {
		g.sweptIdx = 1
	}
}

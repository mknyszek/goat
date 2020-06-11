package stack

import (
	"github.com/mknyszek/goat/simulation"
	"github.com/mknyszek/goat/simulation/toolbox"
)

type stack struct {
	next   *stack
	lo, hi toolbox.Address
}

func (s *stack) size() toolbox.Bytes {
	return s.lo.Diff(s.hi)
}

type stackFreeList struct {
	list *stack
	size toolbox.Bytes
}

func (s *stackFreeList) push(st *stack) {
	st.next = s.list
	s.list = st
	s.size += st.size()
}

func (s *stackFreeList) pop() *stack {
	t := s.list
	if t == nil {
		return nil
	}
	s.list = t.next
	s.size -= t.size()
	return t
}

type stackSpan struct {
	currList   *stackSpanList
	next, prev *stackSpan
	base       toolbox.Address
	list       *stack
	allocCount uint32
	stackSize  toolbox.Bytes
}

type stackSpanList struct {
	first, last *stackSpan
}

func (l *stackSpanList) pushFront(s *stackSpan) {
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

func (l *stackSpanList) pushBack(s *stackSpan) {
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

func (l *stackSpanList) remove(s *stackSpan) {
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

func go114OrderToSize(order uint8) toolbox.Bytes {
	return toolbox.Bytes(1)<<uint64(order) + go114LogMinStackSize
}

const (
	go114LogMinStackSize               = 11
	go114MinStackSize    toolbox.Bytes = 1 << go114LogMinStackSize
	go114NumOrders                     = 4
	go114CacheSize       toolbox.Bytes = go114MinStackSize << go114NumOrders
)

type Go114 struct {
	pageAllocator toolbox.PageAllocator
	cache         map[toolbox.P]*[go114NumOrders]stackFreeList
	pool          [go114NumOrders]stackSpanList
	poolFull      [go114NumOrders]stackSpanList
	large         [64 - go114NumOrders - go114LogMinStackSize]stackFreeList
	gcEnabled     bool
}

func (g *Go114) allocFromPool(ctx toolbox.Context, size toolbox.Bytes) *stack {
	order := size.Log2() - go114LogMinStackSize
	if g.pool[order].first == nil {
		base := g.pageAllocator.AllocPages(ctx, go114CacheSize.Pages(g.pageAllocator.BytesPerPage()))
		s := &stackSpan{
			base:      base,
			stackSize: size,
		}
		for i := toolbox.Bytes(0); i < go114CacheSize; i += size {
			stk := &stack{lo: base.Add(i), hi: base.Add(i + size)}
			stk.next = s.list
			s.list = stk
		}
		g.pool[order].pushFront(s)
	}
	s := g.pool[order].first
	stk := s.list
	s.list = stk.next
	s.allocCount++
	if s.list == nil {
		g.pool[order].remove(s)
		g.poolFull[order].pushFront(s)
	}
	return stk
}

func (g *Go114) freeToPool(ctx toolbox.Context, stk *stack) {
	order := stk.size().Log2() - go114LogMinStackSize
	s := g.pool[order].first
	for s != nil {
		n := s.next
		if stk.lo >= s.base && stk.hi <= s.base.Add(go114CacheSize) {
			stk.next = s.list
			s.list = stk
			s.allocCount--
			if s.allocCount == 0 && !g.gcEnabled {
				g.pool[order].remove(s)
				g.pageAllocator.FreePages(ctx, s.base, go114CacheSize.Pages(g.pageAllocator.BytesPerPage()))
			}
			return
		}
		s = n
	}
	s = g.poolFull[order].first
	for s != nil {
		n := s.next
		if stk.lo >= s.base && stk.hi <= s.base.Add(go114CacheSize) {
			stk.next = s.list
			s.list = stk
			s.allocCount--
			g.poolFull[order].remove(s)
			if s.allocCount == 0 && !g.gcEnabled {
				g.pageAllocator.FreePages(ctx, s.base, go114CacheSize.Pages(g.pageAllocator.BytesPerPage()))
			} else {
				g.pool[order].pushFront(s)
			}
			return
		}
		s = n
	}
	panic("failed to find span for stack")
}

func NewGo114(pa toolbox.PageAllocator) *Go114 {
	if pa.BytesPerPage() != 8192 {
		panic("stack allocator requires a page size of exactly 8192 bytes")
	}
	return &Go114{
		pageAllocator: pa,
		cache:         make(map[toolbox.P]*[go114NumOrders]stackFreeList),
	}
}

func (g *Go114) RegisterStats(s *simulation.Stats) {
	g.pageAllocator.RegisterStats(s)
}

func (g *Go114) AllocStack(ctx toolbox.Context, size toolbox.Bytes) (lo, hi toolbox.Address) {
	if size&(size-1) != 0 {
		panic("size must be a power-of-two")
	}
	if size < go114MinStackSize {
		panic("stack too small")
	}
	if size < go114CacheSize {
		if ctx.P == toolbox.NoP {
			stk := g.allocFromPool(ctx, size)
			lo, hi = stk.lo, stk.hi
		} else {
			order := size.Log2() - go114LogMinStackSize
			cache, ok := g.cache[ctx.P]
			if !ok {
				cache = new([go114NumOrders]stackFreeList)
				g.cache[ctx.P] = cache
			}
			stk := cache[order].pop()
			if stk == nil {
				for cache[order].size < go114CacheSize/2 {
					cache[order].push(g.allocFromPool(ctx, size))
				}
				stk = cache[order].pop()
			}
			lo, hi = stk.lo, stk.hi
		}
	} else {
		order := size.Log2() - go114NumOrders - go114LogMinStackSize
		stk := g.large[order].pop()
		if stk == nil {
			lo = g.pageAllocator.AllocPages(ctx, size.Pages(g.pageAllocator.BytesPerPage()))
			hi = lo.Add(size)
		} else {
			lo, hi = stk.lo, stk.hi
		}
	}
	ctx.Stats.FreeBytes -= uint64(size)
	ctx.Stats.StackBytes += uint64(size)
	return
}

func (g *Go114) FreeStack(ctx toolbox.Context, lo, hi toolbox.Address) {
	size := hi.Diff(lo)
	if size&(size-1) != 0 {
		panic("size must be a power-of-two")
	}
	if size < go114MinStackSize {
		panic("stack too small")
	}
	stk := &stack{lo: lo, hi: hi}
	if size < go114CacheSize {
		if ctx.P == toolbox.NoP {
			g.freeToPool(ctx, stk)
		} else {
			order := size.Log2() - go114LogMinStackSize
			cache, ok := g.cache[ctx.P]
			if !ok {
				cache = new([go114NumOrders]stackFreeList)
				g.cache[ctx.P] = cache
			}
			if cache[order].size >= go114CacheSize {
				for cache[order].size > go114CacheSize/2 {
					g.freeToPool(ctx, cache[order].pop())
				}
			}
			cache[order].push(stk)
		}
	} else {
		order := size.Log2() - go114NumOrders - go114LogMinStackSize
		if g.gcEnabled {
			g.large[order].push(stk)
		} else {
			g.pageAllocator.FreePages(ctx, stk.lo, stk.size().Pages(g.pageAllocator.BytesPerPage()))
		}
	}
	ctx.Stats.FreeBytes += uint64(size)
	ctx.Stats.StackBytes -= uint64(size)
}

func (g *Go114) GCStart(_ toolbox.Context) {
	g.gcEnabled = true
}

func (g *Go114) GCEnd(ctx toolbox.Context) {
	g.gcEnabled = false
	for order := range g.pool {
		c := g.pool[order].first
		for c != nil {
			n := c.next
			if c.allocCount == 0 {
				g.pool[order].remove(c)
				g.pageAllocator.FreePages(ctx, c.base, go114CacheSize.Pages(g.pageAllocator.BytesPerPage()))
			}
			c = n
		}
	}
	for order := range g.large {
		stk := g.large[order].pop()
		for stk != nil {
			g.pageAllocator.FreePages(ctx, stk.lo, stk.size().Pages(g.pageAllocator.BytesPerPage()))
			stk = g.large[order].pop()
		}
	}
}

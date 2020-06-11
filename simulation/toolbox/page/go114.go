package page

import (
	"github.com/mknyszek/goat/simulation"
	"github.com/mknyszek/goat/simulation/toolbox"
)

const go114PageSize = 8192

type go114PageCache struct {
	base  toolbox.Address
	cache uint64
}

var emptyGo114PageCache = go114PageCache{
	cache: ^uint64(0),
}

func (c *go114PageCache) empty() bool {
	return c.cache == ^uint64(0)
}

func (c *go114PageCache) alloc(npages toolbox.Pages) toolbox.Address {
	if c.cache == ^uint64(0) {
		return 0
	}
	offset := toolbox.Pages(0)
	freeSize := toolbox.Pages(0)
	for i := uint(0); i < 64; i++ {
		if c.cache&(uint64(1)<<i) == 0 {
			if freeSize == 0 {
				offset = toolbox.Pages(i)
			}
			freeSize++
			if freeSize >= npages {
				c.cache |= ((uint64(1) << freeSize) - 1) << offset
				return c.base.Add(offset.Bytes(go114PageSize))
			}
		} else {
			freeSize = 0
		}
	}
	return 0
}

const go114ChunkPages = 512
const go114ChunkBytes = go114ChunkPages * go114PageSize

type go114PageBits struct {
	base toolbox.Address
	next *go114PageBits
	prev *go114PageBits
	bits [go114ChunkPages / 64]uint64
}

func (g *go114PageBits) get(i toolbox.Pages) bool {
	return g.bits[i/64]&(uint64(1)<<(i%64)) != 0
}

func (g *go114PageBits) set(i toolbox.Pages) {
	g.bits[i/64] |= uint64(1) << (i % 64)
}

func (g *go114PageBits) clear(i toolbox.Pages) {
	g.bits[i/64] &^= uint64(1) << (i % 64)
}

func (g *go114PageBits) allocCache(i toolbox.Pages) (toolbox.Pages, uint64) {
	idx := i / 64
	cache := g.bits[idx]
	g.bits[idx] = ^uint64(0)
	return idx * 64, cache
}

type go114Pages struct {
	head, tail *go114PageBits
	curr       *go114PageBits
	currIdx    toolbox.Pages
}

func (p *go114Pages) grow(base toolbox.Address, size toolbox.Bytes) {
	if size%go114ChunkBytes != 0 {
		panic("bad size in grow")
	}
	var head, tail *go114PageBits
	for i := toolbox.Bytes(0); i < size; i += go114ChunkBytes {
		n := &go114PageBits{
			base: base.Add(i),
			prev: tail,
		}
		if head == nil {
			head = n
		}
		if tail != nil {
			tail.next = n
		}
		tail = n
	}
	if p.head == nil {
		p.head, p.tail = head, tail
	} else if base < p.head.base {
		tail.next = p.head
		p.head.prev = tail
		p.head = head
	} else if base > p.tail.base {
		p.tail.next = head
		head.prev = p.tail
		p.tail = tail
	} else {
		c := p.head
		var p *go114PageBits
		for c != nil {
			if base > c.base {
				break
			}
			p = c
			c = c.next
		}
		p.next = head
		head.prev = p
		c.prev = tail
		tail.next = c
	}
	if p.curr == nil || base < p.curr.base {
		p.curr = head
		p.currIdx = 0
	}
}

func (p *go114Pages) findFirstFree() (*go114PageBits, toolbox.Pages) {
	curr, currIdx := p.curr, p.currIdx
findFirstFree:
	for curr != nil {
		for i := currIdx; i < go114ChunkPages; i++ {
			if !curr.get(i) {
				currIdx = i
				break findFirstFree
			}
		}
		curr = curr.next
		currIdx = 0
	}
	p.curr, p.currIdx = curr, currIdx
	return curr, currIdx
}

func (p *go114Pages) find(n toolbox.Pages) (*go114PageBits, toolbox.Pages) {
	curr, currIdx := p.findFirstFree()
	if curr == nil {
		return nil, 0
	}
	basePtr := curr
	baseIdx := currIdx
	size := toolbox.Pages(0)
	for curr != nil {
		for i := currIdx; i < go114ChunkPages; i++ {
			if !curr.get(i) {
				if size == 0 {
					basePtr = curr
					baseIdx = i
				}
				size++
				if size >= n {
					return basePtr, baseIdx
				}
			} else {
				size = 0
			}
		}
		curr = curr.next
		currIdx = 0
	}
	return nil, 0
}

func (p *go114Pages) alloc(basePtr *go114PageBits, baseIdx toolbox.Pages, size toolbox.Pages) {
	for basePtr != nil {
		for i := baseIdx; i < go114ChunkPages; i++ {
			basePtr.set(i)
			size--
			if size == 0 {
				return
			}
		}
		basePtr = basePtr.next
		baseIdx = 0
	}
	if size > 0 {
		panic("ran out of pages to alloc")
	}
}

func (p *go114Pages) free(basePtr *go114PageBits, baseIdx toolbox.Pages, size toolbox.Pages) {
	if (basePtr == p.curr && baseIdx < p.currIdx) || basePtr.base < p.curr.base {
		p.curr = basePtr
		p.currIdx = baseIdx
	}
	for basePtr != nil {
		for i := baseIdx; i < go114ChunkPages; i++ {
			if !basePtr.get(i) {
				panic("attempted to double free page")
			}
			basePtr.clear(i)
			size--
			if size == 0 {
				return
			}
		}
		basePtr = basePtr.next
		baseIdx = 0
	}
	if size > 0 {
		panic("ran out of pages to free")
	}
}

func (p *go114Pages) allocToCache() *go114PageCache {
	curr, currIdx := p.findFirstFree()
	if curr == nil {
		return &emptyGo114PageCache
	}
	baseIdx, cache := curr.allocCache(currIdx)
	return &go114PageCache{
		base:  curr.base.Add(baseIdx.Bytes(go114PageSize)),
		cache: cache,
	}
}

const go114ArenaSize toolbox.Bytes = toolbox.Bytes(1 << 26)

type Go114 struct {
	addressSpace toolbox.AddressSpace
	pageCaches   map[toolbox.P]*go114PageCache
	pages        go114Pages
}

func NewGo114(a toolbox.AddressSpace) *Go114 {
	return &Go114{
		addressSpace: a,
		pageCaches:   make(map[toolbox.P]*go114PageCache),
	}
}

func (g *Go114) RegisterStats(s *simulation.Stats) {
	g.addressSpace.RegisterStats(s)
}

func (g *Go114) BytesPerPage() toolbox.Bytes {
	return go114PageSize
}

func (g *Go114) AllocPages(ctx toolbox.Context, n toolbox.Pages) toolbox.Address {
	if ctx.P != toolbox.NoP && n < 16 {
		cache, ok := g.pageCaches[ctx.P]
		if !ok || cache.empty() {
			cache = g.pages.allocToCache()
			g.pageCaches[ctx.P] = cache
		}
		if base := cache.alloc(n); base != 0 {
			return base
		}
	}
	basePtr, baseIdx := g.pages.find(n)
	if basePtr == nil {
		ask := n.Bytes(go114PageSize)
		ask = ask.AlignUp(go114ArenaSize)
		g.pages.grow(g.addressSpace.MapAligned(ctx, ask, go114ArenaSize))
		basePtr, baseIdx = g.pages.find(n)
		if basePtr == nil {
			panic("out of memory?")
		}
	}
	g.pages.alloc(basePtr, baseIdx, n)
	addr := basePtr.base.Add(baseIdx.Bytes(go114PageSize))
	return addr
}

func (g *Go114) FreePages(ctx toolbox.Context, addr toolbox.Address, size toolbox.Pages) {
	if addr%go114PageSize != 0 {
		panic("unaligned free address")
	}
	c := g.pages.tail
	for c != nil {
		if addr >= c.base {
			break
		}
		c = c.prev
	}
	idx := addr.Diff(c.base).Pages(go114PageSize)
	g.pages.free(c, idx, size)
}

package toolbox

import "github.com/mknyszek/goat/simulation"

type AddressSpace48 struct {
	base     Address
	pageSize Bytes
}

func NewAddressSpace48(pageSize Bytes) *AddressSpace48 {
	if (pageSize)&(pageSize-1) != 0 {
		panic("page size must be a power-of-two")
	}
	return &AddressSpace48{
		base:     0xc00000000000,
		pageSize: pageSize,
	}
}

func (s *AddressSpace48) RegisterStats(_ *simulation.Stats) {}

func (s *AddressSpace48) MapAligned(ctx Context, size, align Bytes) (Address, Bytes) {
	size = size.AlignUp(s.pageSize)
	base := s.base.AlignUp(align)
	s.base = base.Add(size)
	ctx.Stats.FreeBytes += uint64(size)
	return base, size
}

package toolbox

// AddressSet is a set of addresses laid out for efficient
// memory use and access.
type AddressSet struct {
	// m is a 4-level radix structure.
	//
	// The bottom level is a bitmap, with one bit per byte.
	m [1 << 16]*[1 << 16]*[1 << 16]*[(1 << 16) / 8]uint8
}

// Add adds a new address to the AddressSet.
//
// Returns true on success. That is, if the address
// was not already present in the map.
func (a *AddressSet) Add(addr uint64) bool {
	l1 := &a.m[addr>>48]
	if *l1 == nil {
		*l1 = new([1 << 16]*[1 << 16]*[(1 << 16) / 8]uint8)
	}
	l2 := &((*l1)[(addr>>32)&0xffff])
	if *l2 == nil {
		*l2 = new([1 << 16]*[(1 << 16) / 8]uint8)
	}
	l3 := &((*l2)[(addr>>16)&0xffff])
	if *l3 == nil {
		*l3 = new([(1 << 16) / 8]uint8)
	}
	c := *l3
	i := addr & 0xffff
	mask := uint8(1) << (i % 8)
	idx := i / 8
	if c[idx]&mask != 0 {
		return false
	}
	c[idx] |= mask
	return true
}

// Remove removes an address from the AddressSet.
//
// Returns true on success. That is, if the address
// was present in the map.
func (a *AddressSet) Remove(addr uint64) bool {
	l1 := a.m[addr>>48]
	if l1 == nil {
		return false
	}
	l2 := l1[(addr>>32)&0xffff]
	if l2 == nil {
		return false
	}
	l3 := l2[(addr>>16)&0xffff]
	if l3 == nil {
		return false
	}
	i := addr & 0xffff
	mask := uint8(1) << (i % 8)
	idx := i / 8
	if l3[idx]&mask == 0 {
		return false
	}
	l3[idx] &^= mask
	return true
}

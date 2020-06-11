package toolbox

import (
	"math/bits"

	"github.com/mknyszek/goat/simulation"
)

// Bytes represents an amount of bytes.
type Bytes uint64

// AlignUp rounds b up to align. align must be a power-of-two.
func (b Bytes) AlignUp(align Bytes) Bytes {
	if align&(align-1) != 0 {
		panic("alignment must be a power-of-two")
	}
	return (b + align - 1) &^ (align - 1)
}

// AlignDown rounds b down to align. align must be a power-of-two.
func (b Bytes) AlignDown(align Bytes) Bytes {
	if align&(align-1) != 0 {
		panic("alignment must be a power-of-two")
	}
	return b &^ (align - 1)
}

// Pages returns the amount of perPage-sized pages required to hold b bytes.
func (b Bytes) Pages(perPage Bytes) Pages {
	return Pages(b.AlignUp(perPage) / perPage)
}

// Log2 returns the base-2 logarithm (rounded down) of b.
func (b Bytes) Log2() uint8 {
	if b == 0 {
		panic("log2 of 0")
	}
	return uint8(bits.Len64(uint64(b))) - 1
}

// Address represents a virtual address.
type Address uint64

// AlignUp rounds a up to align. align must be a power-of-two.
func (a Address) AlignUp(align Bytes) Address {
	return Address(Bytes(a).AlignUp(align))
}

// AlignUp rounds a down to align. align must be a power-of-two.
func (a Address) AlignDown(align Bytes) Address {
	return Address(Bytes(a).AlignDown(align))
}

// Add adds a byte offset to an address.
func (a Address) Add(b Bytes) Address {
	return a + Address(b)
}

// Diff returns the absolute difference between a and b.
func (a Address) Diff(b Address) Bytes {
	if a < b {
		return Bytes(b - a)
	}
	return Bytes(a - b)
}

// Pages represents an amount of pages. The amount of bytes
// per page is determined contextually, usually from
// (PageAllocator).BytesPerPage().
type Pages uint64

// Bytes returns the maximum amount of bytes that can be held within p pages,
// given perPage bytes per page.
func (p Pages) Bytes(perPage Bytes) Bytes {
	return Bytes(p) * perPage
}

// P is the ID of a P in the Go runtime, a virtual processor.
type P int32

// NoP is the P ID for a lack of P.
const NoP P = -1

// Context represents a context for the entirety of the simulation.
// It contains references to state that need to be accessible to
// all parts of the simulation.
type Context struct {
	P
	*simulation.Stats
}

// Simulation is a marker interface for a simulation, and also
// provides a common method for registering implementation-specific
// statistics.
type Simulation interface {
	// RegisterStats may register new implementation-specific stats
	// with the simulation.Stats.
	//
	// RegisterStats must be an idempodent operation, just like
	// (*simulation.Stats).RegisterOther().
	RegisterStats(*simulation.Stats)
}

// ObjectAllocator represents an interface to a simulated object
// allocator.
type ObjectAllocator interface {
	Simulation

	// AllocObject allocates an object, updating statistics
	// in the context, and returns the base address for the
	// new object.
	AllocObject(c Context, size Bytes, array, noscan bool) Address

	// DeadObject marks the object slot that starts at the given
	// address as dead, but doesn't necessarily free that object.
	//
	// After GCEnd is called on this ObjectAllocator, there must
	// be a stream of DeadObject calls before anything else happens
	// in order to mark all objects not marked as dead.
	// The larger granularity memory space held for that object
	// (if applicable) must be freed eagerly in order to ensure
	// a consistent object sweeping policy across implementations.
	DeadObject(Context, Address)

	// GCStart signals the start of a GC cycle to the allocator.
	GCStart(Context)

	// GCStart signals the end of a GC cycle to the allocator.
	GCEnd(Context)
}

// StackAllocator represents an interface to a simulated stack
// allocator.
type StackAllocator interface {
	Simulation

	// AllocStack allocates a new simulated goroutine stack of
	// size Bytes and returns the address range for the stack.
	// It also updates statistics in the context.
	AllocStack(Context, Bytes) (lo Address, hi Address)

	// FreeStack frees a simulated goroutine stack that starts
	// at the given address (the lo value from AllocStack).
	// It also updates statistics in the context.
	FreeStack(ctx Context, lo, hi Address)

	// GCStart signals the start of a GC cycle to the allocator.
	GCStart(Context)

	// GCStart signals the end of a GC cycle to the allocator.
	GCEnd(Context)
}

// StackAllocator represents an interface to a simulated stack
// allocator.
type PageAllocator interface {
	Simulation

	// BytesPerPage returns the number of bytes per page
	// in this allocator. Must always be a power of two.
	BytesPerPage() Bytes

	// AllocPages allocates contiguous pages and returns the
	// the base address of the range. It also updates statistics
	// in the context.
	AllocPages(Context, Pages) Address

	// FreePages frees the contiguous pages starting at the given
	// address. It also updates statistics in the context.
	FreePages(Context, Address, Pages)
}

// AddressSpace represents a simulated address space which hands
// out addresses.
type AddressSpace interface {
	Simulation

	// MapAligned simulates an OS's mmap or equivalent except
	// that the region is aligned to its size.
	// Updates statistics in the context.
	MapAligned(ctx Context, size, align Bytes) (Address, Bytes)
}

package toolbox

import (
	"github.com/mknyszek/goat"
	"github.com/mknyszek/goat/simulation"
)

// stack represents a goroutine stack.
type stack struct {
	lo, hi Address
}

// Simulator implements the simulation.Simulator interface for a
// complete Go runtime simulation implementation which has both
// an ObjectAllocator and StackAllocator. These allocators are
// assumed to share the same address space, so allocations returned
// by the two must never overlap.
type Simulator struct {
	oa            ObjectAllocator
	sa            StackAllocator
	collectEvents bool
	gcEvents      []goat.Event
	idToAddress   map[uint64]Address
	idToStack     map[uint64]stack
}

// NewSimulator constructs a new simulator from the given allocators.
//
// The allocators should share an address space and their allocations
// must never overlap.
func NewSimulator(oa ObjectAllocator, sa StackAllocator) *Simulator {
	return &Simulator{
		oa:          oa,
		sa:          sa,
		idToAddress: make(map[uint64]Address),
		idToStack:   make(map[uint64]stack),
	}
}

// RegisterStats registers additional implementation-specific statistics
// with the simulation.Stats.
func (s *Simulator) RegisterStats(stats *simulation.Stats) {
	s.oa.RegisterStats(stats)
	s.sa.RegisterStats(stats)
}

// Process implements the simulation.Simulator interface.
func (s *Simulator) Process(ev goat.Event, stats *simulation.Stats) {
	if s.collectEvents {
		// Find all the free events so we can mark objects as dead.
		// This lets the object allocator know which objects are dead
		// up-front so that it can control its own sweep scheduling.
		ctx := Context{P(ev.P), stats}
		switch ev.Kind {
		case goat.EventFree:
			addr := s.idToAddress[ev.Address]
			delete(s.idToAddress, ev.Address)
			s.oa.DeadObject(ctx, addr)
			return
		case goat.EventGCStart:
			s.collectEvents = false
		default:
			s.gcEvents = append(s.gcEvents, ev)
			return
		}
	}
	if len(s.gcEvents) != 0 {
		// Drain all the events collected, this happens on a GC start.
		// All these events happen while a GC is not currently executing,
		// so there should be no GC events and all the free events should
		// have been filtered out above.
		for _, ev := range s.gcEvents {
			ctx := Context{P(ev.P), stats}
			switch ev.Kind {
			case goat.EventStackAlloc:
				lo, hi := s.sa.AllocStack(ctx, Bytes(ev.Size))
				s.idToStack[ev.Address] = stack{lo, hi}
			case goat.EventStackFree:
				stk := s.idToStack[ev.Address]
				delete(s.idToStack, ev.Address)
				s.sa.FreeStack(ctx, stk.lo, stk.hi)
			case goat.EventAlloc:
				s.idToAddress[ev.Address] = s.oa.AllocObject(ctx, Bytes(ev.Size), ev.Array, ev.PointerFree)
			default:
				panic("unexpected gc event")
			}
		}
		s.gcEvents = s.gcEvents[:0]
	}
	stats.Timestamp = ev.Timestamp
	// Handle an event; we know now that there's a GC running.
	ctx := Context{P(ev.P), stats}
	switch ev.Kind {
	case goat.EventStackAlloc:
		lo, hi := s.sa.AllocStack(ctx, Bytes(ev.Size))
		s.idToStack[ev.Address] = stack{lo, hi}
	case goat.EventStackFree:
		stk := s.idToStack[ev.Address]
		delete(s.idToStack, ev.Address)
		s.sa.FreeStack(ctx, stk.lo, stk.hi)
	case goat.EventAlloc:
		s.idToAddress[ev.Address] = s.oa.AllocObject(ctx, Bytes(ev.Size), ev.Array, ev.PointerFree)
	case goat.EventFree:
		// This isn't generally possible with most GC implementations,
		// but we let this case go through to support simulating implementations
		// which may free objects concurrently with marking.
		addr := s.idToAddress[ev.Address]
		delete(s.idToAddress, ev.Address)
		s.oa.DeadObject(ctx, addr)
	case goat.EventGCStart:
		s.oa.GCStart(ctx)
		s.sa.GCStart(ctx)
	case goat.EventGCEnd:
		// The end of GC means we know which objects are live and dead.
		// Collect all object deaths next time we come around.
		s.oa.GCEnd(ctx)
		s.sa.GCEnd(ctx)
		stats.GCCycles++
		s.collectEvents = true
	}
}

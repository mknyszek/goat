package simulation

import (
	"sort"

	"github.com/mknyszek/goat"
)

// Stats is a sample of statistics produced by the
// simulator.
type Stats struct {
	// Timestamp is the time in CPU ticks for the most
	// recent event processed by the simulator.
	Timestamp uint64

	// GCCycles is the number of complete GC cycles that
	// have passed since the simulator started.
	GCCycles uint64

	// Allocs is the total number of allocations processed by
	// the simulator.
	Allocs uint64

	// Frees is the total number of frees processed by the
	// simulator.
	Frees uint64

	// ObjectBytes is the amount of memory in bytes
	// occupied by live objects.
	ObjectBytes uint64

	// StackBytes is the amount of memory in bytes
	// occupied by live stacks.
	StackBytes uint64

	// UnusedBytes is the amount of memory in bytes
	// which is not occupied by live memory, but
	// that otherwise cannot be used to do so (e.g. fragmentation).
	UnusedBytes uint64

	// FreeBytes is the amount of memory in bytes
	// which is not occupied by live memory, but that may
	// be used to do so in the future.
	FreeBytes uint64

	// other represents statistics which are unique to the
	// implementation, usually representing a breakdown of
	// other statistics, or something else entirely.
	other map[string]uint64
}

// NewStats creates a new valid Stats object.
//
// Must be used instead of constructing a Stats object directly,
// since there are unexported fields which may need to be initialized.
func NewStats() *Stats {
	return &Stats{
		other: make(map[string]uint64),
	}
}

// OtherStats returns a list of registered implementation-specific statistics.
func (s *Stats) OtherStats() []string {
	names := make([]string, 0, len(s.other))
	for name := range s.other {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// GetOther returns the value for a implementation-specific statistic
// by name. Returns 0 if the statistic is not registered.
func (s *Stats) GetOther(name string) uint64 {
	return s.other[name]
}

// RegisterOther registers a new implementation-specific statistic.
//
// This operation is idempotent and safe to perform again, even after
// a statistic has been modified.
func (s *Stats) RegisterOther(name string) {
	if _, ok := s.other[name]; !ok {
		s.other[name] = 0
	}
}

// AddOther adds an amount to the value to a implementation-specific statistic.
// Panics if the statistic has not been registered.
func (s *Stats) AddOther(name string, amount uint64) {
	if val, ok := s.other[name]; ok {
		s.other[name] = val + amount
	} else {
		panic("attempted to add to non-existing stat")
	}
}

// SubOther subtracts an amount from the value of a implementation-specific
// statistic. Panics if the statistic has not been registered.
func (s *Stats) SubOther(name string, amount uint64) {
	if val, ok := s.other[name]; ok {
		s.other[name] = val - amount
	} else {
		panic("attempted to subtract from non-existing stat")
	}
}

// Simulator describes a heap allocation simulator for Go.
type Simulator interface {
	// RegisterStats offers the simulator an opportunity to
	// register any additional statistics before processing.
	RegisterStats(*Stats)

	// Process feeds another heap allocation trace event
	// into the simulator.
	Process(goat.Event, *Stats)
}

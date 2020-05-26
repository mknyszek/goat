// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goat

// EventKind indicates what kind of allocation trace event
// is captured and returned.
type EventKind uint8

const (
	EventBad        EventKind = iota
	EventAlloc                // Allocation.
	EventFree                 // Free.
	EventGCStart              // GC sweep termination.
	EventGCEnd                // GC mark termination.
	EventStackAlloc           // Stack allocation.
	EventStackFree            // Stack free.
)

// Event represents a single allocation trace event.
type Event struct {
	// Timestamp is the time in non-normalized CPU ticks
	// for this event.
	Timestamp uint64

	// Address is the address for the allocation or free.
	// Only valid when Kind == EventAlloc, Kind == EventFree,
	// Kind == EventStackAlloc, Kind == EventStackFree.
	Address uint64

	// Size indicates the size of the allocation.
	// Only valid when Kind == EventAlloc or Kind == EventStackAlloc.
	Size uint64

	// P indicates which processor generated the event.
	// Valid for all events.
	P int32

	// Array indicates whether an allocation was for
	// an array type.
	Array bool

	// Kind indicates what kind of event this is.
	// This may be assumed to always be valid.
	Kind EventKind
}

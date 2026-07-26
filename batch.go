package digestr

import (
	"strconv"
	"time"
)

// FlushReason records why a [Batch] closed.
type FlushReason int

const (
	// Elapsed means the window's time trigger fired: [Digester.Due] was called
	// at or after the window's DueAt.
	Elapsed FlushReason = iota

	// SizeReached means the window hit [Config.MaxSize] and [Digester.Add]
	// flushed it early, before its time trigger.
	SizeReached
)

// String returns the reason's name for diagnostics.
func (r FlushReason) String() string {
	switch r {
	case Elapsed:
		return "Elapsed"
	case SizeReached:
		return "SizeReached"
	default:
		return "FlushReason(" + strconv.Itoa(int(r)) + ")"
	}
}

// Batch is one flushed digest: the aggregated events for a key, ready for the
// caller to render and deliver. digestr never delivers it.
type Batch[E any] struct {
	// Key is the grouping key these events shared.
	Key string

	// Events are the digested events in arrival order, with duplicates already
	// collapsed if a [Config.Dedup] was set. The slice is the caller's to keep
	// and mutate.
	Events []E

	// OpenedAt is when the window's first event arrived. DueAt is OpenedAt plus
	// the window; for a SizeReached flush it is the time trigger the batch beat.
	OpenedAt time.Time
	DueAt    time.Time

	// Reason is why the batch flushed.
	Reason FlushReason
}

// Window is an observability view of one open digest, returned by
// [Digester.Pending]. It carries no events — only metadata — so it is cheap and
// never generic.
type Window struct {
	// Key is the grouping key.
	Key string

	// Size is how many events the window currently holds (post-dedup).
	Size int

	// OpenedAt is when the window opened; DueAt is when its time trigger fires.
	OpenedAt time.Time
	DueAt    time.Time
}

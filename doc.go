// Package digestr is a pure-Go notification digest / batching engine: many
// events per recipient in, one batched notification out, on a window the caller
// controls. It has zero dependencies — only the standard library.
//
// digestr does the grouping-and-windowing computation that notification
// platforms monetize as a workflow step, and only that. It does not send,
// store, template, or own channels. It owns no goroutines and no timers: the
// caller advances the clock by passing now to [Digester.Add] and [Digester.Due],
// which makes the engine deterministic, trivially testable, and safe to persist.
//
//	d, _ := digestr.New(digestr.Config[Event]{
//		Key:    func(e Event) string { return e.Recipient },
//		Window: time.Minute,
//	})
//	d.Add(t0, Event{Recipient: "amy", Text: "liked your post"})
//	d.Add(t0.Add(10*time.Second), Event{Recipient: "amy", Text: "commented"})
//	for _, b := range d.Due(t0.Add(time.Minute)) {
//		send(b.Key, b.Events) // one digest of two events
//	}
//
// # The model
//
// digestr holds one open [Window] per active key. The first event for a key
// opens a window [T, T+Window); later events for that key join it. A window
// flushes exactly once, into a [Batch], on whichever trigger fires first:
//
//   - Time. [Digester.Due](now) flushes every window whose DueAt is at or
//     before now (Reason [Elapsed]). The boundary is inclusive: a window
//     opened at T is due at exactly T+Window.
//   - Size. If [Config.MaxSize] is positive and a window reaches that many
//     distinct events, the [Digester.Add] that fills it flushes it immediately
//     (Reason [SizeReached]) — Knock's batch_size, which bounds latency for a
//     hot key. A later Due does not re-emit those events.
//
// # Strategies
//
// [Fixed] digests every event: the first opens a window and the rest join.
// [Backoff] digests only repeats — a lone event for a key with no recent send
// passes straight through undigested (returned by Add as immediate), and only a
// follow-up within [Config.Lookback] opens a window. The recency horizon is
// half-open: a send exactly Lookback ago is not recent. Both a passthrough
// delivery and a batch flush count as a "send" that starts the horizon, so a
// cold key's second event within Lookback digests — see the correction note in
// docs/DESIGN.md.
//
// # The Add contract
//
// [Digester.Add] returns at most one of two results, never both. immediate
// carries a Backoff passthrough (nil under Fixed, or whenever a window is
// already open); flushed carries a Batch only when that Add reached MaxSize.
// Both nil is the ordinary case: the event joined an open window, or was dropped
// as a duplicate. Add never time-flushes.
//
// # Dedup
//
// A non-nil [Config.Dedup] collapses duplicates within one window: an event
// whose non-empty id a still-open window has already seen is dropped, and the
// batch keeps the single copy. An empty ("") id is never deduplicated, and
// dedup never spans windows.
//
// # Determinism
//
// [Digester.Due] sorts flushed batches by key, and events keep arrival order,
// so the same construction over the same now produces identical output on every
// run — no map-iteration nondeterminism. Draining is idempotent: a second Due
// with the same now returns nothing.
//
// # Out-of-order time
//
// The caller drives the clock, so digestr defends against a non-monotonic one.
// It tracks the highest now it has observed; any earlier now handed to Add or
// Due is clamped forward to it. Windows therefore never carry a negative span,
// Due never un-fires, and no event is lost.
//
// # Persistence
//
// An open window is state that must survive a crash. digestr keeps it in memory
// and makes it the caller's to persist rather than dragging in a database.
// [Digester.Snapshot] returns a serializable point-in-time copy of the open
// windows, the Backoff markers, and the clock; [Restore] rebuilds a Digester
// from a fresh [Config] and that snapshot, resuming exactly. Only data persists
// — never the Config's function fields.
//
// # Concurrency
//
// A *[Digester] is safe for concurrent Add and Due; every method takes an
// internal mutex, and Snapshot is a consistent point-in-time copy.
//
// # Errors
//
// [New] and [Restore] return typed sentinels matchable with [errors.Is]:
// [ErrNoKey], [ErrBadWindow], [ErrBadSize], [ErrBadLookback] and
// [ErrBadStrategy].
//
// # Not in scope
//
// No delivery, channels, templates, preferences, quiet hours, goroutines,
// timers, or database. digestr computes batches; everything else is the
// caller's. See docs/DESIGN.md, including the "Phase 1 as built" note recording
// the decisions this implementation made where the design left a choice open.
package digestr

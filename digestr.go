package digestr

import (
	"sort"
	"sync"
	"time"
)

// Digester is a pure, clock-driven digest state machine. It holds one open
// window per active key and starts no goroutines or timers: the caller advances
// time by passing now to [Digester.Add] and [Digester.Due]. The zero value is
// not usable; construct one with [New] or [Restore].
//
// A *Digester is safe for concurrent use — every method takes an internal
// mutex — so it can sit on a notification hot path shared by many producers.
type Digester[E any] struct {
	mu    sync.Mutex
	cfg   Config[E]
	open  map[string]*openWindow[E] // exactly one open window per key
	last  map[string]time.Time      // Backoff only: per-key last send instant
	clock time.Time                 // monotonic high-water mark of observed now
}

// openWindow is one accumulating digest. seen is created lazily and only when a
// Dedup function is configured.
type openWindow[E any] struct {
	key      string
	events   []E
	seen     map[string]struct{}
	openedAt time.Time
	dueAt    time.Time
}

// New builds a Digester from cfg, returning a typed sentinel (see the package
// errors) if the configuration is unusable.
func New[E any](cfg Config[E]) (*Digester[E], error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Digester[E]{
		cfg:  cfg,
		open: make(map[string]*openWindow[E]),
		last: make(map[string]time.Time),
	}, nil
}

// advance clamps now forward to the highest instant the Digester has observed,
// keeping the engine's clock monotonically non-decreasing even if a caller
// supplies an out-of-order now. It must be called under the lock.
func (d *Digester[E]) advance(now time.Time) time.Time {
	if now.Before(d.clock) {
		return d.clock
	}
	d.clock = now
	return now
}

// Add offers event e at instant now.
//
// It returns at most one of two results, never both:
//
//   - immediate holds the event when Backoff passes it straight through
//     (a lone event with no recent send for its key); it is nil otherwise.
//   - flushed holds a Batch when this Add filled a window to [Config.MaxSize]
//     and flushed it early (Reason [SizeReached]); it is nil otherwise.
//
// When both are nil the event was buffered into an open window — the common
// case — or dropped as a duplicate of one already in that window. Add never
// time-flushes; the time trigger belongs to [Digester.Due].
//
// If now precedes an instant the Digester has already observed, it is clamped
// forward, so windows never carry a negative span and events are never lost.
func (d *Digester[E]) Add(now time.Time, e E) (immediate []E, flushed *Batch[E]) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Read the caller's Key and Dedup functions before touching any state, so
	// that if either panics the Digester is left exactly as it was — the clock
	// has not advanced and no window has changed. Add propagates such a panic to
	// the caller; it does not recover it.
	key := d.cfg.Key(e)
	var dedupID string
	if d.cfg.Dedup != nil {
		dedupID = d.cfg.Dedup(e)
	}

	now = d.advance(now)

	w := d.open[key]
	if w == nil {
		// No open window for this key. Under Backoff, a key with no recent send
		// is not a repeat, so the event passes through and opens nothing.
		if d.cfg.Strategy == Backoff && !d.recent(key, now) {
			d.last[key] = now
			return []E{e}, nil
		}
		w = &openWindow[E]{
			key:      key,
			openedAt: now,
			dueAt:    now.Add(d.cfg.Window),
		}
		d.open[key] = w
	}

	// Collapse a duplicate within this window; a dropped duplicate cannot push
	// the window over MaxSize, so the size check below sees only distinct events.
	if dedupID != "" {
		if w.seen == nil {
			w.seen = make(map[string]struct{})
		}
		if _, dup := w.seen[dedupID]; dup {
			return nil, nil
		}
		w.seen[dedupID] = struct{}{}
	}
	w.events = append(w.events, e)

	if d.cfg.MaxSize > 0 && len(w.events) >= d.cfg.MaxSize {
		b := d.flush(w, SizeReached, now)
		return nil, &b
	}
	return nil, nil
}

// recent reports whether a send for key happened strictly less than Lookback
// before now. Exactly Lookback ago is not recent: the horizon is the half-open
// interval [now-Lookback, now). It must be called under the lock.
func (d *Digester[E]) recent(key string, now time.Time) bool {
	last, ok := d.last[key]
	if !ok {
		return false
	}
	return now.Sub(last) < d.cfg.Lookback
}

// Due flushes and returns every window whose time trigger has fired at or
// before now — that is, whose DueAt is at or before now — closing each. Results
// are ordered by key (one window is open per key), so a fixed now over
// identical history yields identical output every run. Draining is idempotent:
// calling Due again with the same now returns nothing, because the due windows
// are already gone.
//
// now is clamped forward if it precedes an instant already observed.
func (d *Digester[E]) Due(now time.Time) []Batch[E] {
	d.mu.Lock()
	defer d.mu.Unlock()
	now = d.advance(now)

	var batches []Batch[E]
	for _, w := range d.open {
		if !now.Before(w.dueAt) { // now >= dueAt
			// Anchor the Backoff recency marker at the poll instant, not the
			// window's logical dueAt: the batch is surfaced for delivery now,
			// so "a similar notification was sent recently" is measured from
			// now. On an on-time poll now == dueAt, so this only differs when a
			// poll runs late — where anchoring at the stale dueAt would wrongly
			// let a quick repeat pass through instead of digesting.
			batches = append(batches, d.flush(w, Elapsed, now))
		}
	}
	// At most one window is open per key, so flushed batches have distinct keys
	// and ordering by key alone is a total, deterministic order.
	sort.Slice(batches, func(i, j int) bool { return batches[i].Key < batches[j].Key })

	// Forget Backoff markers that can no longer make any key "recent": a send
	// at or beyond Lookback ago behaves exactly like no send at all, so pruning
	// it changes no decision and bounds live memory to recently active keys.
	if d.cfg.Strategy == Backoff {
		for k, t := range d.last {
			if now.Sub(t) >= d.cfg.Lookback {
				delete(d.last, k)
			}
		}
	}
	return batches
}

// flush removes a window, records the send for Backoff recency, and returns the
// batch. sendAt is the instant the batch is surfaced for delivery — the poll
// now, for both an Elapsed and a SizeReached flush — which is what the Backoff
// recency horizon is measured from. It must be called under the lock.
func (d *Digester[E]) flush(w *openWindow[E], reason FlushReason, sendAt time.Time) Batch[E] {
	delete(d.open, w.key)
	if d.cfg.Strategy == Backoff {
		d.last[w.key] = sendAt
	}
	return Batch[E]{
		Key:      w.key,
		Events:   w.events,
		OpenedAt: w.openedAt,
		DueAt:    w.dueAt,
		Reason:   reason,
	}
}

// Pending reports every open window as a metadata-only [Window], ordered by
// key. Use it for observability or to decide when next to call [Due].
func (d *Digester[E]) Pending() []Window {
	d.mu.Lock()
	defer d.mu.Unlock()

	out := make([]Window, 0, len(d.open))
	for _, w := range d.open {
		out = append(out, Window{
			Key:      w.key,
			Size:     len(w.events),
			OpenedAt: w.openedAt,
			DueAt:    w.dueAt,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

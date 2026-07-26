package digestr

import (
	"fmt"
	"sort"
	"time"
)

// Snapshot is a serializable, point-in-time copy of a [Digester]'s state: its
// open windows, its Backoff recency markers, and its clock. It is a plain
// exported struct with stable JSON tags, so a caller can persist it with
// encoding/json (or anything else) on shutdown and hand it to [Restore] on
// boot to resume exactly where it left off.
//
// Only data is captured — never the [Config]'s function fields. The caller
// supplies a fresh Config to Restore. If the event type E is to survive a JSON
// round-trip, its own fields must be exported, as with any encoding/json value.
//
// The slices are sorted deterministically by key, so two snapshots of equal
// state serialize byte-for-byte alike.
type Snapshot[E any] struct {
	Windows  []WindowState[E] `json:"windows,omitempty"`
	LastSend []KeyTime        `json:"lastSend,omitempty"`
	Clock    time.Time        `json:"clock"`
}

// WindowState is one open window inside a [Snapshot]. The dedup set is not
// stored: it is rebuilt from Events on [Restore] using the fresh Config's Dedup,
// since the surviving events already are exactly the distinct ids seen.
type WindowState[E any] struct {
	Key      string    `json:"key"`
	Events   []E       `json:"events"`
	OpenedAt time.Time `json:"openedAt"`
	DueAt    time.Time `json:"dueAt"`
}

// KeyTime is a per-key instant in a [Snapshot] — used for the Backoff last-send
// markers.
type KeyTime struct {
	Key  string    `json:"key"`
	Time time.Time `json:"time"`
}

// Snapshot returns a consistent point-in-time copy of the Digester, taken under
// the lock. Every slice is freshly allocated, so the caller may marshal or
// mutate the result while the Digester keeps running.
func (d *Digester[E]) Snapshot() Snapshot[E] {
	d.mu.Lock()
	defer d.mu.Unlock()

	s := Snapshot[E]{Clock: d.clock}

	if len(d.open) > 0 {
		s.Windows = make([]WindowState[E], 0, len(d.open))
		for _, w := range d.open {
			events := make([]E, len(w.events))
			copy(events, w.events)
			s.Windows = append(s.Windows, WindowState[E]{
				Key:      w.key,
				Events:   events,
				OpenedAt: w.openedAt,
				DueAt:    w.dueAt,
			})
		}
		// One window per key, so sorting by key alone is deterministic.
		sort.Slice(s.Windows, func(i, j int) bool { return s.Windows[i].Key < s.Windows[j].Key })
	}

	if len(d.last) > 0 {
		s.LastSend = make([]KeyTime, 0, len(d.last))
		for k, t := range d.last {
			s.LastSend = append(s.LastSend, KeyTime{Key: k, Time: t})
		}
		sort.Slice(s.LastSend, func(i, j int) bool {
			return s.LastSend[i].Key < s.LastSend[j].Key
		})
	}
	return s
}

// Restore rebuilds a Digester from a fresh cfg and a Snapshot, resuming the
// exact window state the snapshot captured. cfg is validated as in [New] and
// its error is returned if it is unusable; the snapshot itself is trusted.
//
// The Config must be supplied afresh because a Snapshot carries no functions.
// Restoring with a different Config (a wider Window, a different Dedup) resumes
// the persisted events under the new behaviour — that is the caller's choice.
func Restore[E any](cfg Config[E], s Snapshot[E]) (*Digester[E], error) {
	d, err := New(cfg)
	if err != nil {
		return nil, err
	}
	d.clock = s.Clock

	for _, ws := range s.Windows {
		// A key names at most one window; two windows sharing a key would make
		// the map silently keep one and drop the other's events — a lost
		// notification. A [Snapshot] never emits duplicate keys, so this can
		// only come from a corrupt or hand-built one, and it is rejected rather
		// than swallowed.
		if _, dup := d.open[ws.Key]; dup {
			return nil, fmt.Errorf("%w: snapshot has two windows for key %q", ErrBadSnapshot, ws.Key)
		}
		w := &openWindow[E]{
			key:      ws.Key,
			openedAt: ws.OpenedAt,
			dueAt:    ws.DueAt,
		}
		w.events = make([]E, len(ws.Events))
		copy(w.events, ws.Events)
		if cfg.Dedup != nil {
			for _, e := range w.events {
				if id := cfg.Dedup(e); id != "" {
					if w.seen == nil {
						w.seen = make(map[string]struct{})
					}
					w.seen[id] = struct{}{}
				}
			}
		}
		d.open[ws.Key] = w
	}

	if cfg.Strategy == Backoff {
		for _, kt := range s.LastSend {
			d.last[kt.Key] = kt.Time
		}
	}
	return d, nil
}

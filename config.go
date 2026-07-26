package digestr

import (
	"strconv"
	"time"
)

// Strategy selects how digestr decides whether an event begins (or joins) a
// digest window at all.
type Strategy int

const (
	// Fixed opens a window on the first event for a key and digests every
	// event for that key until the window flushes — Novu's "Regular" digest.
	// Every event is digested; nothing passes through.
	Fixed Strategy = iota

	// Backoff digests only when events repeat: a lone event for a key with no
	// recent send passes straight through undigested, and only a follow-up
	// within Lookback opens a window — Novu's "when events repeat". It requires
	// a positive [Config.Lookback].
	Backoff
)

// String returns the strategy's name for diagnostics.
func (s Strategy) String() string {
	switch s {
	case Fixed:
		return "Fixed"
	case Backoff:
		return "Backoff"
	default:
		return "Strategy(" + strconv.Itoa(int(s)) + ")"
	}
}

// Config describes a [Digester]. E is the caller's event payload; digestr is
// generic over it and never inspects it beyond the key and optional dedup id
// the caller derives from it.
//
// The function fields are behaviour, not data: they are supplied fresh at [New]
// (or [Restore]) and are never serialized by [Snapshot]. Two Digesters restored
// from the same snapshot with the same Config behave identically.
type Config[E any] struct {
	// Key groups events; it is required. All events sharing a key digest
	// together. A common key is the recipient id, or recipient plus a
	// sub-topic (Knock's concat(recipient_id, batch_key)).
	Key func(E) string

	// Window is how long a window stays open. A window opened at instant T is
	// due, and flushes, at exactly T+Window. Required to be positive.
	Window time.Duration

	// Strategy is [Fixed] (default) or [Backoff].
	Strategy Strategy

	// MaxSize, when positive, is a count ceiling (Knock's batch_size): a window
	// that reaches MaxSize distinct events flushes immediately on [Digester.Add]
	// without waiting for the time trigger. Zero disables the size trigger.
	MaxSize int

	// Lookback is the Backoff recency horizon and is used only when Strategy is
	// [Backoff]. A key is "recent" — and so a new event opens a digest rather
	// than passing through — when a send happened strictly less than Lookback
	// ago. Required to be positive under Backoff.
	Lookback time.Duration

	// Dedup, when non-nil, collapses duplicates within a single window: if it
	// returns a non-empty id that a still-open window has already seen, the
	// event is dropped and the batch keeps the one copy. An empty ("") id is
	// never deduplicated. Dedup does not span windows.
	Dedup func(E) string
}

// validate reports the first configuration problem, or nil.
func (c Config[E]) validate() error {
	if c.Key == nil {
		return ErrNoKey
	}
	if c.Window <= 0 {
		return ErrBadWindow
	}
	if c.MaxSize < 0 {
		return ErrBadSize
	}
	switch c.Strategy {
	case Fixed:
	case Backoff:
		if c.Lookback <= 0 {
			return ErrBadLookback
		}
	default:
		return ErrBadStrategy
	}
	return nil
}

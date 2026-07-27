//go:build js && wasm

package main

import (
	"encoding/json"
	"fmt"
)

// A scenario is a named story: a digester configuration plus a script of timed
// steps that, replayed in order, teaches one idea in about fifteen seconds.
// digestr is a stateful engine, so unlike a stateless demo a scenario is not
// just inputs — it is a small timeline the page replays step by step, firing an
// event or advancing the clock, so the visitor watches windows fill and batches
// emit. The numbers are pinned to the library's own semantics, so a scenario
// reproduces a figure from the README exactly.
type scenario struct {
	Name   string         `json:"name"`
	Title  string         `json:"title"`
	Blurb  string         `json:"blurb"`
	Note   string         `json:"note"`
	Config scenarioConfig `json:"config"`
	Steps  []scenarioStep `json:"steps"`
}

// scenarioConfig is the config the page loads into the control strip before
// replaying the steps. Its JSON shape matches the newSession request.
type scenarioConfig struct {
	Strategy  string  `json:"strategy"`
	WindowSec float64 `json:"windowSec"`
	MaxSize   int     `json:"maxSize"`
	Lookback  float64 `json:"lookbackSec"`
	Dedup     bool    `json:"dedup"`
}

// scenarioStep is one replay action: an "event" offered at AtSec, or a "clock"
// advance to AtSec that drains due windows.
type scenarioStep struct {
	Type      string  `json:"type"` // "event" | "clock"
	AtSec     float64 `json:"atSec"`
	Recipient string  `json:"recipient,omitempty"`
	Kind      string  `json:"kind,omitempty"`
	Body      string  `json:"body,omitempty"`
	ID        string  `json:"id,omitempty"`
}

func ev(at float64, recip, kind, body string) scenarioStep {
	return scenarioStep{Type: "event", AtSec: at, Recipient: recip, Kind: kind, Body: body}
}

func clock(at float64) scenarioStep { return scenarioStep{Type: "clock", AtSec: at} }

var scenarios = map[string]*scenario{
	"chatty-backoff": {
		Name:  "chatty-backoff",
		Title: "Chatty service (backoff wins)",
		Blurb: "A lone alert goes out at once; a repeat within the look-back opens a digest, so a flurry becomes one batch, not one page per event.",
		Note: "Backoff, 30s window, 120s look-back. The first alert has no recent send, so it passes straight " +
			"through — delivered immediately, no window. The next alert arrives within the look-back, so it opens a " +
			"digest; the rest join it. Advance the clock past 38s and the four repeats flush as one batch: 5 events → " +
			"1 immediate + 1 digest, not 5 pages.",
		Config: scenarioConfig{Strategy: "backoff", WindowSec: 30, MaxSize: 0, Lookback: 120},
		Steps: []scenarioStep{
			ev(0, "on-call", "deploy", "deploy #1418 started"),
			ev(8, "on-call", "deploy", "pod crash-looping"),
			ev(16, "on-call", "deploy", "pod crash-looping"),
			ev(24, "on-call", "deploy", "readiness probe failing"),
			ev(32, "on-call", "deploy", "rollback started"),
			clock(40),
		},
	},

	"size-flush-5": {
		Name:  "size-flush-5",
		Title: "Size flush at 5",
		Blurb: "A hot key hits the size ceiling and flushes early — the batch that fills to MaxSize emits at once, without waiting for the clock.",
		Note: "Fixed, MaxSize 5, a long 10-minute window. Five events land on one recipient; the fifth fills the " +
			"window to the ceiling and Add flushes it immediately (Reason SizeReached), long before the time trigger. " +
			"This is Knock's batch_size — it bounds latency for a chatty key. 5 events → 1 batch.",
		Config: scenarioConfig{Strategy: "fixed", WindowSec: 600, MaxSize: 5},
		Steps: []scenarioStep{
			ev(0, "team-ops", "alert", "CPU above 90%"),
			ev(3, "team-ops", "alert", "memory above 90%"),
			ev(6, "team-ops", "alert", "disk above 90%"),
			ev(9, "team-ops", "alert", "latency p99 breached"),
			ev(12, "team-ops", "alert", "error rate spiking"),
			clock(15),
		},
	},

	"quiet-then-burst": {
		Name:  "quiet-then-burst",
		Title: "Quiet then burst",
		Blurb: "A calm trickle flushes small on the clock; a burst hits the size ceiling first. Watch both triggers fire on the same key.",
		Note: "Fixed, 20s window, MaxSize 6. Two quiet events flush on the clock as a batch of two (Reason Elapsed). " +
			"Then a burst of eight: the sixth fills the window and flushes early (SizeReached), and the last two open a " +
			"fresh window that flushes on time. 10 events → 3 batches, size and time both firing.",
		Config: scenarioConfig{Strategy: "fixed", WindowSec: 20, MaxSize: 6},
		Steps: []scenarioStep{
			ev(0, "digest-me", "post", "new comment on your photo"),
			ev(4, "digest-me", "post", "someone liked your photo"),
			clock(22),
			ev(30, "digest-me", "post", "3 new followers"),
			ev(31, "digest-me", "post", "mention in a thread"),
			ev(32, "digest-me", "post", "reply to your comment"),
			ev(33, "digest-me", "post", "someone shared your post"),
			ev(34, "digest-me", "post", "new comment"),
			ev(35, "digest-me", "post", "new like"),
			ev(36, "digest-me", "post", "new follower"),
			ev(37, "digest-me", "post", "mention in a thread"),
			clock(58),
		},
	},

	"two-recipients": {
		Name:  "two-recipients",
		Title: "Two recipients, independent windows",
		Blurb: "Each recipient owns one window keyed on its own first event, so two people digest on their own clocks and flush as separate batches.",
		Note: "Fixed, 30s window. Amy's window opens on her first event at 0s (due 30s); Ben's opens on his at 5s " +
			"(due 35s). Their events never mix — the Key groups by recipient. Advance past both deadlines and each " +
			"flushes its own batch: 5 events → 2 batches, one per recipient, ordered by key.",
		Config: scenarioConfig{Strategy: "fixed", WindowSec: 30, MaxSize: 0},
		Steps: []scenarioStep{
			ev(0, "amy", "social", "liked your post"),
			ev(5, "ben", "social", "commented"),
			ev(10, "amy", "social", "followed you"),
			ev(18, "ben", "social", "shared your post"),
			ev(22, "amy", "social", "mentioned you"),
			clock(40),
		},
	},
}

// scenarioOrder fixes the presentation order in the UI, the backoff flagship
// first.
var scenarioOrder = []string{"chatty-backoff", "size-flush-5", "quiet-then-burst", "two-recipients"}

func scenarioList() []map[string]any {
	out := make([]map[string]any, 0, len(scenarioOrder))
	for _, name := range scenarioOrder {
		sc, ok := scenarios[name]
		if !ok {
			continue
		}
		out = append(out, map[string]any{"name": sc.Name, "title": sc.Title, "blurb": sc.Blurb})
	}
	return out
}

// handleLoadScenario returns a scenario's full preset by name. The name may
// arrive as a bare string or as {"name": ...}.
func handleLoadScenario(in []byte) (any, error) {
	name := scenarioName(in)
	sc, ok := scenarios[name]
	if !ok {
		return nil, badArg("unknown_scenario", fmt.Sprintf("unknown scenario %q", name))
	}
	return sc, nil
}

func scenarioName(in []byte) string {
	s := string(in)
	if s == "" || s == "{}" {
		return ""
	}
	if s[0] != '{' {
		return s
	}
	var obj struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(in, &obj); err == nil {
		return obj.Name
	}
	return ""
}

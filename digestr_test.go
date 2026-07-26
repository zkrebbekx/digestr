package digestr

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// note is the event payload used throughout the tests. Its fields are exported
// so a Snapshot of []note survives an encoding/json round-trip.
type note struct {
	To   string
	ID   string
	Tag  int
	Text string
}

func key(n note) string  { return n.To }
func idOf(n note) string { return n.ID }

var t0 = time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return t0.Add(d) }

func mustNew(t *testing.T, cfg Config[note]) *Digester[note] {
	t.Helper()
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: unexpected error: %v", err)
	}
	return d
}

func texts(b Batch[note]) []string {
	out := make([]string, len(b.Events))
	for i, e := range b.Events {
		out[i] = e.Text
	}
	return out
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- Golden vector 1: Fixed, time flush -----------------------------------

func TestGoldenFixedTimeFlush(t *testing.T) {
	t.Run("Given a Fixed digester with a 60s window", func(t *testing.T) {
		d := mustNew(t, Config[note]{Key: key, Window: 60 * time.Second})
		t.Run("When three events for r1 arrive at t=0,10,20", func(t *testing.T) {
			d.Add(at(0), note{To: "r1", Text: "a"})
			d.Add(at(10*time.Second), note{To: "r1", Text: "b"})
			d.Add(at(20*time.Second), note{To: "r1", Text: "c"})

			t.Run("Then Due at t=59 is empty", func(t *testing.T) {
				if got := d.Due(at(59 * time.Second)); len(got) != 0 {
					t.Fatalf("Due(59) = %v, want empty", got)
				}
			})
			t.Run("Then Due at t=60 flushes one batch of three, Elapsed", func(t *testing.T) {
				got := d.Due(at(60 * time.Second))
				if len(got) != 1 {
					t.Fatalf("Due(60) returned %d batches, want 1", len(got))
				}
				b := got[0]
				if b.Key != "r1" || b.Reason != Elapsed {
					t.Fatalf("batch key=%q reason=%v, want r1/Elapsed", b.Key, b.Reason)
				}
				if want := []string{"a", "b", "c"}; !eq(texts(b), want) {
					t.Fatalf("events = %v, want %v (arrival order)", texts(b), want)
				}
				if !b.OpenedAt.Equal(at(0)) || !b.DueAt.Equal(at(60*time.Second)) {
					t.Fatalf("OpenedAt=%v DueAt=%v, want %v / %v", b.OpenedAt, b.DueAt, at(0), at(60*time.Second))
				}
			})
			t.Run("Then a second Due at t=60 is idempotent and empty", func(t *testing.T) {
				if got := d.Due(at(60 * time.Second)); len(got) != 0 {
					t.Fatalf("second Due(60) = %v, want empty", got)
				}
			})
		})
	})
}

// --- Golden vector 2: Fixed, immediate open --------------------------------

func TestGoldenFixedImmediateOpen(t *testing.T) {
	t.Run("Given a Fixed digester with a 60s window", func(t *testing.T) {
		d := mustNew(t, Config[note]{Key: key, Window: 60 * time.Second})
		t.Run("When the first event arrives off any grid at t=13s", func(t *testing.T) {
			d.Add(at(13*time.Second), note{To: "r1", Text: "a"})
			t.Run("Then DueAt is firstEvent+Window, not a wall-clock grid", func(t *testing.T) {
				p := d.Pending()
				if len(p) != 1 {
					t.Fatalf("Pending = %v, want one window", p)
				}
				if !p[0].OpenedAt.Equal(at(13*time.Second)) || !p[0].DueAt.Equal(at(73*time.Second)) {
					t.Fatalf("OpenedAt=%v DueAt=%v, want 13s / 73s", p[0].OpenedAt, p[0].DueAt)
				}
			})
		})
	})
}

// --- Golden vector 3: Size trigger -----------------------------------------

func TestGoldenSizeTrigger(t *testing.T) {
	t.Run("Given a Fixed digester, 1h window, MaxSize 5", func(t *testing.T) {
		d := mustNew(t, Config[note]{Key: key, Window: time.Hour, MaxSize: 5})
		t.Run("When five events for r1 arrive within the hour", func(t *testing.T) {
			for i := 0; i < 4; i++ {
				imm, fl := d.Add(at(time.Duration(i)*time.Second), note{To: "r1", Text: string(rune('a' + i))})
				if imm != nil || fl != nil {
					t.Fatalf("Add #%d returned (%v,%v), want (nil,nil)", i, imm, fl)
				}
			}
			imm, fl := d.Add(at(4*time.Second), note{To: "r1", Text: "e"})

			t.Run("Then the fifth Add flushes a batch of five, SizeReached", func(t *testing.T) {
				if imm != nil {
					t.Fatalf("immediate = %v, want nil", imm)
				}
				if fl == nil {
					t.Fatalf("flushed = nil, want a batch")
				}
				if fl.Reason != SizeReached {
					t.Fatalf("reason = %v, want SizeReached", fl.Reason)
				}
				if want := []string{"a", "b", "c", "d", "e"}; !eq(texts(*fl), want) {
					t.Fatalf("events = %v, want %v", texts(*fl), want)
				}
				if !fl.DueAt.Equal(at(time.Hour)) {
					t.Fatalf("DueAt = %v, want the beaten time trigger at 1h", fl.DueAt)
				}
			})
			t.Run("Then a later Due does not re-emit them", func(t *testing.T) {
				if got := d.Due(at(2 * time.Hour)); len(got) != 0 {
					t.Fatalf("Due(2h) = %v, want empty", got)
				}
			})
		})
	})
}

// --- Golden vector 4: Backoff passthrough ----------------------------------

func TestGoldenBackoffPassthrough(t *testing.T) {
	t.Run("Given a Backoff digester with no recent send for r1", func(t *testing.T) {
		d := mustNew(t, Config[note]{Key: key, Window: 60 * time.Second, Strategy: Backoff, Lookback: 5 * time.Minute})
		t.Run("When a lone event for r1 arrives", func(t *testing.T) {
			imm, fl := d.Add(at(0), note{To: "r1", Text: "a"})
			t.Run("Then it is returned immediately and opens no window", func(t *testing.T) {
				if fl != nil {
					t.Fatalf("flushed = %v, want nil", fl)
				}
				if len(imm) != 1 || imm[0].Text != "a" {
					t.Fatalf("immediate = %v, want [a]", imm)
				}
				if p := d.Pending(); len(p) != 0 {
					t.Fatalf("Pending = %v, want empty (no window opened)", p)
				}
			})
		})
	})
}

// --- Golden vector 5: Backoff repeat ---------------------------------------

func TestGoldenBackoffRepeat(t *testing.T) {
	t.Run("Given a Backoff digester where r1 was sent (passthrough) at t=0", func(t *testing.T) {
		d := mustNew(t, Config[note]{Key: key, Window: 60 * time.Second, Strategy: Backoff, Lookback: 5 * time.Minute})
		imm, _ := d.Add(at(0), note{To: "r1", Text: "first"})
		if len(imm) != 1 {
			t.Fatalf("setup: first event should pass through, got %v", imm)
		}
		t.Run("When a repeat arrives within Lookback (t=30s)", func(t *testing.T) {
			imm, fl := d.Add(at(30*time.Second), note{To: "r1", Text: "second"})
			t.Run("Then it opens a digest window instead of passing through", func(t *testing.T) {
				if imm != nil || fl != nil {
					t.Fatalf("Add returned (%v,%v), want (nil,nil) — buffered into a window", imm, fl)
				}
				p := d.Pending()
				if len(p) != 1 || p[0].Key != "r1" || p[0].Size != 1 {
					t.Fatalf("Pending = %v, want one r1 window of size 1", p)
				}
				if !p[0].DueAt.Equal(at(90 * time.Second)) {
					t.Fatalf("DueAt = %v, want 90s (opened at the repeat instant)", p[0].DueAt)
				}
			})
		})
	})

	t.Run("Given a Backoff digester that produced a real flush", func(t *testing.T) {
		d := mustNew(t, Config[note]{Key: key, Window: 60 * time.Second, Strategy: Backoff, Lookback: 5 * time.Minute})
		d.Add(at(0), note{To: "r1", Text: "first"})          // passthrough, send@0
		d.Add(at(10*time.Second), note{To: "r1", Text: "b"}) // repeat opens window
		flush := d.Due(at(70 * time.Second))                 // window due at 70s -> real flush
		if len(flush) != 1 {
			t.Fatalf("setup: expected one flushed batch, got %d", len(flush))
		}
		t.Run("When another event arrives within Lookback of that flush", func(t *testing.T) {
			imm, fl := d.Add(at(80*time.Second), note{To: "r1", Text: "c"})
			t.Run("Then the repeat chain continues and a new window opens", func(t *testing.T) {
				if imm != nil || fl != nil {
					t.Fatalf("Add returned (%v,%v), want a buffered repeat", imm, fl)
				}
				if p := d.Pending(); len(p) != 1 {
					t.Fatalf("Pending = %v, want one window", p)
				}
			})
		})
	})
}

// --- Golden vector 6: Dedup ------------------------------------------------

func TestGoldenDedup(t *testing.T) {
	t.Run("Given a Fixed digester with dedup by id", func(t *testing.T) {
		d := mustNew(t, Config[note]{Key: key, Window: 60 * time.Second, Dedup: idOf})
		t.Run("When the same id arrives twice in one window", func(t *testing.T) {
			d.Add(at(0), note{To: "r1", ID: "x", Text: "a"})
			imm, fl := d.Add(at(1*time.Second), note{To: "r1", ID: "x", Text: "a-again"})
			t.Run("Then the duplicate collapses and the batch carries one copy", func(t *testing.T) {
				if imm != nil || fl != nil {
					t.Fatalf("duplicate Add returned (%v,%v), want (nil,nil)", imm, fl)
				}
				got := d.Due(at(60 * time.Second))
				if len(got) != 1 || len(got[0].Events) != 1 || got[0].Events[0].Text != "a" {
					t.Fatalf("batch = %v, want a single 'a'", got)
				}
			})
		})
	})
}

// --- Golden vector 7: Grouping independence --------------------------------

func TestGoldenGroupingIndependence(t *testing.T) {
	t.Run("Given events for two recipients on their own windows", func(t *testing.T) {
		d := mustNew(t, Config[note]{Key: key, Window: 60 * time.Second})
		d.Add(at(0), note{To: "r1", Text: "r1a"})
		d.Add(at(5*time.Second), note{To: "r2", Text: "r2a"})
		d.Add(at(6*time.Second), note{To: "r1", Text: "r1b"})
		t.Run("When both windows are due", func(t *testing.T) {
			got := d.Due(at(65 * time.Second))
			t.Run("Then they flush as separate batches, sorted by key", func(t *testing.T) {
				if len(got) != 2 {
					t.Fatalf("got %d batches, want 2", len(got))
				}
				if got[0].Key != "r1" || got[1].Key != "r2" {
					t.Fatalf("keys = %q,%q, want r1,r2", got[0].Key, got[1].Key)
				}
				if !eq(texts(got[0]), []string{"r1a", "r1b"}) {
					t.Fatalf("r1 events = %v", texts(got[0]))
				}
				if !eq(texts(got[1]), []string{"r2a"}) {
					t.Fatalf("r2 events = %v", texts(got[1]))
				}
			})
		})
	})
}

// --- Golden vector 8: Determinism ------------------------------------------

func TestGoldenDeterminism(t *testing.T) {
	t.Run("Given identical construction and history", func(t *testing.T) {
		build := func() []Batch[note] {
			d := mustNew(t, Config[note]{Key: key, Window: 60 * time.Second})
			for i, r := range []string{"rc", "ra", "rb", "ra", "rc"} {
				d.Add(at(time.Duration(i)*time.Second), note{To: r, Text: r})
			}
			return d.Due(at(2 * time.Minute))
		}
		t.Run("When Due runs repeatedly over fresh digesters", func(t *testing.T) {
			first := build()
			t.Run("Then every run returns the same batches in the same order", func(t *testing.T) {
				for run := 0; run < 50; run++ {
					got := build()
					if len(got) != len(first) {
						t.Fatalf("run %d: %d batches, want %d", run, len(got), len(first))
					}
					for i := range got {
						if got[i].Key != first[i].Key || !eq(texts(got[i]), texts(first[i])) {
							t.Fatalf("run %d batch %d differs: %v vs %v", run, i, got[i], first[i])
						}
					}
				}
				if len(first) != 3 || first[0].Key != "ra" || first[1].Key != "rb" || first[2].Key != "rc" {
					t.Fatalf("keys = %q, want ra,rb,rc", []string{first[0].Key, first[1].Key, first[2].Key})
				}
			})
		})
	})
}

// --- Edge: zero events -----------------------------------------------------

func TestDueOnEmpty(t *testing.T) {
	t.Run("Given a fresh digester with no events", func(t *testing.T) {
		d := mustNew(t, Config[note]{Key: key, Window: time.Minute})
		t.Run("When Due is called", func(t *testing.T) {
			t.Run("Then it returns nothing and does not error", func(t *testing.T) {
				if got := d.Due(at(time.Hour)); len(got) != 0 {
					t.Fatalf("Due = %v, want empty", got)
				}
				if p := d.Pending(); len(p) != 0 {
					t.Fatalf("Pending = %v, want empty", p)
				}
			})
		})
	})
}

// --- Edge: MaxSize boundary (N-1, N, N+1) ----------------------------------

func TestSizeBoundary(t *testing.T) {
	t.Run("Given a Fixed digester with MaxSize 5", func(t *testing.T) {
		t.Run("When only four events arrive, the window stays open", func(t *testing.T) {
			d := mustNew(t, Config[note]{Key: key, Window: time.Hour, MaxSize: 5})
			for i := 0; i < 4; i++ {
				if _, fl := d.Add(at(0), note{To: "r1", Text: "x"}); fl != nil {
					t.Fatalf("Add #%d flushed early", i)
				}
			}
			if p := d.Pending(); len(p) != 1 || p[0].Size != 4 {
				t.Fatalf("Pending = %v, want one window of size 4", p)
			}
		})
		t.Run("When exactly five arrive, the fifth flushes", func(t *testing.T) {
			d := mustNew(t, Config[note]{Key: key, Window: time.Hour, MaxSize: 5})
			var last *Batch[note]
			for i := 0; i < 5; i++ {
				_, last = d.Add(at(0), note{To: "r1", Text: "x"})
			}
			if last == nil || len(last.Events) != 5 {
				t.Fatalf("fifth Add flushed %v, want a batch of 5", last)
			}
			if p := d.Pending(); len(p) != 0 {
				t.Fatalf("Pending = %v, want empty after size flush", p)
			}
		})
		t.Run("When a sixth arrives, it opens a fresh window", func(t *testing.T) {
			d := mustNew(t, Config[note]{Key: key, Window: time.Hour, MaxSize: 5})
			for i := 0; i < 5; i++ {
				d.Add(at(0), note{To: "r1", Text: "x"})
			}
			imm, fl := d.Add(at(1*time.Second), note{To: "r1", Text: "sixth"})
			if imm != nil || fl != nil {
				t.Fatalf("sixth Add returned (%v,%v), want a fresh buffered window", imm, fl)
			}
			p := d.Pending()
			if len(p) != 1 || p[0].Size != 1 || !p[0].OpenedAt.Equal(at(1*time.Second)) {
				t.Fatalf("Pending = %v, want one fresh window of size 1 opened at 1s", p)
			}
		})
	})

	t.Run("Given MaxSize 1, every event flushes on arrival", func(t *testing.T) {
		d := mustNew(t, Config[note]{Key: key, Window: time.Hour, MaxSize: 1})
		_, fl := d.Add(at(0), note{To: "r1", Text: "solo"})
		if fl == nil || len(fl.Events) != 1 {
			t.Fatalf("flushed = %v, want a batch of 1", fl)
		}
	})
}

// --- Edge: out-of-order time (clamping) ------------------------------------

func TestOutOfOrderTimeClamps(t *testing.T) {
	t.Run("Given a window opened at t=60s", func(t *testing.T) {
		d := mustNew(t, Config[note]{Key: key, Window: 60 * time.Second})
		d.Add(at(60*time.Second), note{To: "r1", Text: "a"}) // window [60s,120s)
		t.Run("When a later event arrives with an earlier now (t=10s)", func(t *testing.T) {
			d.Add(at(10*time.Second), note{To: "r1", Text: "b"})
			t.Run("Then it is clamped forward and joins the same window", func(t *testing.T) {
				p := d.Pending()
				if len(p) != 1 || p[0].Size != 2 {
					t.Fatalf("Pending = %v, want one window of size 2 (no negative-span window)", p)
				}
				if !p[0].DueAt.Equal(at(120 * time.Second)) {
					t.Fatalf("DueAt = %v, want unchanged 120s", p[0].DueAt)
				}
			})
			t.Run("Then a Due with an earlier now is clamped and does not under-fire", func(t *testing.T) {
				if got := d.Due(at(5 * time.Second)); len(got) != 0 {
					t.Fatalf("Due(5s) clamped to 60s = %v, want empty (not yet due)", got)
				}
			})
			t.Run("Then the window flushes on time at 120s as a batch of two", func(t *testing.T) {
				got := d.Due(at(120 * time.Second))
				if len(got) != 1 || !eq(texts(got[0]), []string{"a", "b"}) {
					t.Fatalf("batch = %v, want [a b]", got)
				}
			})
		})
	})
}

// --- Edge: Backoff Lookback boundary ---------------------------------------

func TestBackoffLookbackBoundary(t *testing.T) {
	look := 5 * time.Minute
	t.Run("Given a Backoff key sent (passthrough) at t=0", func(t *testing.T) {
		newAtZero := func() *Digester[note] {
			d := mustNew(t, Config[note]{Key: key, Window: time.Minute, Strategy: Backoff, Lookback: look})
			d.Add(at(0), note{To: "r1", Text: "first"})
			return d
		}
		t.Run("When a repeat arrives just inside Lookback", func(t *testing.T) {
			d := newAtZero()
			imm, _ := d.Add(at(look-time.Nanosecond), note{To: "r1", Text: "b"})
			t.Run("Then it digests (opens a window)", func(t *testing.T) {
				if imm != nil {
					t.Fatalf("immediate = %v, want nil (should digest just inside Lookback)", imm)
				}
				if p := d.Pending(); len(p) != 1 {
					t.Fatalf("Pending = %v, want one window", p)
				}
			})
		})
		t.Run("When a repeat arrives exactly at the Lookback boundary", func(t *testing.T) {
			d := newAtZero()
			imm, _ := d.Add(at(look), note{To: "r1", Text: "b"})
			t.Run("Then it passes through (the send has aged out at exactly Lookback)", func(t *testing.T) {
				if len(imm) != 1 {
					t.Fatalf("immediate = %v, want a passthrough at exactly Lookback", imm)
				}
				if p := d.Pending(); len(p) != 0 {
					t.Fatalf("Pending = %v, want no window", p)
				}
			})
		})
	})
}

// --- Edge: dedup where every event duplicates the first --------------------

func TestDedupAllDuplicates(t *testing.T) {
	t.Run("Given a window whose events all share one id", func(t *testing.T) {
		d := mustNew(t, Config[note]{Key: key, Window: time.Minute, Dedup: idOf})
		for i := 0; i < 5; i++ {
			d.Add(at(time.Duration(i)*time.Second), note{To: "r1", ID: "same", Text: "e"})
		}
		t.Run("When the window flushes", func(t *testing.T) {
			got := d.Due(at(time.Minute))
			t.Run("Then the batch holds exactly one event", func(t *testing.T) {
				if len(got) != 1 || len(got[0].Events) != 1 {
					t.Fatalf("batch = %v, want a single event", got)
				}
			})
		})
	})
}

// --- Edge: empty dedup id is never deduplicated ----------------------------

func TestEmptyDedupIDNeverCollapses(t *testing.T) {
	t.Run("Given a dedup function that returns empty for some events", func(t *testing.T) {
		d := mustNew(t, Config[note]{Key: key, Window: time.Minute, Dedup: idOf})
		d.Add(at(0), note{To: "r1", ID: "", Text: "a"})
		d.Add(at(1*time.Second), note{To: "r1", ID: "", Text: "b"})
		t.Run("When the window flushes", func(t *testing.T) {
			got := d.Due(at(time.Minute))
			t.Run("Then both empty-id events are kept", func(t *testing.T) {
				if len(got) != 1 || !eq(texts(got[0]), []string{"a", "b"}) {
					t.Fatalf("batch = %v, want [a b] (empty ids never collapse)", got)
				}
			})
		})
	})
}

// --- Stringers -------------------------------------------------------------

func TestStringers(t *testing.T) {
	t.Run("Given the enum types", func(t *testing.T) {
		t.Run("When Stringed", func(t *testing.T) {
			t.Run("Then known values name themselves and unknowns are labelled", func(t *testing.T) {
				cases := []struct {
					got, want string
				}{
					{Fixed.String(), "Fixed"},
					{Backoff.String(), "Backoff"},
					{Strategy(9).String(), "Strategy(9)"},
					{Elapsed.String(), "Elapsed"},
					{SizeReached.String(), "SizeReached"},
					{FlushReason(9).String(), "FlushReason(9)"},
				}
				for _, c := range cases {
					if c.got != c.want {
						t.Errorf("String = %q, want %q", c.got, c.want)
					}
				}
			})
		})
	})
}

// --- Concurrency: race-clean Add/Due/Snapshot ------------------------------

func TestConcurrentAddDue(t *testing.T) {
	t.Run("Given many producers adding while others drain and snapshot", func(t *testing.T) {
		d := mustNew(t, Config[note]{Key: key, Window: time.Hour})
		const producers, perProducer = 8, 500
		var added int64
		var wg sync.WaitGroup

		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				for i := 0; i < perProducer; i++ {
					atomic.AddInt64(&added, 1)
					d.Add(at(0), note{To: string(rune('a' + p%4)), Tag: p*perProducer + i})
				}
			}(p)
		}
		// Concurrent readers/drainers to exercise the lock (nothing is due yet).
		for r := 0; r < 3; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 200; i++ {
					d.Due(at(0))
					d.Pending()
					_ = d.Snapshot()
				}
			}()
		}
		wg.Wait()

		t.Run("When everything is finally drained", func(t *testing.T) {
			var flushed int
			for _, b := range d.Due(at(2 * time.Hour)) {
				flushed += len(b.Events)
			}
			t.Run("Then no event was lost and none was duplicated", func(t *testing.T) {
				if int64(flushed) != atomic.LoadInt64(&added) {
					t.Fatalf("flushed %d events, added %d", flushed, added)
				}
			})
		})
	})
}

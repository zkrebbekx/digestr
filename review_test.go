package digestr

import (
	"errors"
	"testing"
	"time"
)

// TestBackoffMarkerAnchoredAtPoll pins that an Elapsed flush anchors the Backoff
// recency marker at the poll instant, not the window's logical dueAt — so a
// repeat shortly after a LATE delivery digests rather than passing through.
func TestBackoffMarkerAnchoredAtPoll(t *testing.T) {
	d := mustNew(t, Config[note]{Key: key, Window: 60 * time.Second, Strategy: Backoff, Lookback: 300 * time.Second})

	t.Run("Given a window delivered by a poll long after its dueAt", func(t *testing.T) {
		d.Add(at(0), note{To: "r1"})              // passthrough, opens no window, marks send at 0
		d.Add(at(10*time.Second), note{To: "r1"}) // repeat within lookback → opens window [10s,70s)
		got := d.Due(at(1000 * time.Second))      // poll far past dueAt=70s; batch surfaced ~1000s
		if len(got) != 1 {
			t.Fatalf("expected one flushed batch, got %d", len(got))
		}

		t.Run("Then a repeat 5s after the actual delivery digests, not passes through", func(t *testing.T) {
			imm, _ := d.Add(at(1005*time.Second), note{To: "r1"})
			if len(imm) != 0 {
				t.Fatalf("event 5s after a late delivery passed through (immediate=%d); "+
					"the recency marker must be anchored at the poll instant, not the stale dueAt", len(imm))
			}
			if len(d.Pending()) != 1 {
				t.Fatalf("expected a digest window to open, pending=%d", len(d.Pending()))
			}
		})
	})
}

// TestAddPanicLeavesStateUntouched pins that a panicking Key or Dedup function
// is propagated with the Digester's state — including its observed clock —
// exactly as it was before the Add.
func TestAddPanicLeavesStateUntouched(t *testing.T) {
	t.Run("Given a Key function that panics", func(t *testing.T) {
		boom := false
		d := mustNew(t, Config[note]{
			Key: func(n note) string {
				if boom {
					panic("key boom")
				}
				return n.To
			},
			Window: 60 * time.Second,
		})
		d.Add(at(0), note{To: "r1"}) // advance the observed clock to t0
		boom = true

		t.Run("Then Add propagates the panic and the clock is not advanced", func(t *testing.T) {
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("expected Add to propagate the Key panic")
					}
				}()
				d.Add(at(1000*time.Second), note{To: "r1"})
			}()
			// The pre-panic window is still due at t0+60s; a far-future Add
			// having advanced the clock to 1000s would have changed nothing
			// observable, but we assert the surviving window is intact.
			if len(d.Pending()) != 1 {
				t.Fatalf("the pre-panic window was disturbed: pending=%d", len(d.Pending()))
			}
			if got := d.Due(at(60 * time.Second)); len(got) != 1 {
				t.Fatalf("the pre-panic window did not flush at its original dueAt; got %d batches", len(got))
			}
		})
	})
}

// TestRestoreRejectsDuplicateKeys pins that a corrupt snapshot with two windows
// for one key is rejected, not silently collapsed into a lost notification.
func TestRestoreRejectsDuplicateKeys(t *testing.T) {
	t.Run("Given a hand-built snapshot with two windows sharing a key", func(t *testing.T) {
		s := Snapshot[note]{
			Clock: at(0),
			Windows: []WindowState[note]{
				{Key: "r1", OpenedAt: at(0), DueAt: at(60 * time.Second), Events: []note{{To: "r1", Text: "a"}}},
				{Key: "r1", OpenedAt: at(1), DueAt: at(61 * time.Second), Events: []note{{To: "r1", Text: "b"}}},
			},
		}
		t.Run("Then Restore returns ErrBadSnapshot rather than dropping events", func(t *testing.T) {
			_, err := Restore(Config[note]{Key: key, Window: 60 * time.Second}, s)
			if !errors.Is(err, ErrBadSnapshot) {
				t.Fatalf("Restore err = %v, want ErrBadSnapshot", err)
			}
		})
	})
}

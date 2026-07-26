package digestr

import (
	"encoding/json"
	"testing"
	"time"
)

// roundTrip snapshots d and returns a digester restored from a JSON round-trip
// of that snapshot.
func roundTrip(t *testing.T, cfg Config[note], d *Digester[note]) *Digester[note] {
	t.Helper()
	return restoreVia(t, cfg, d.Snapshot())
}

// restoreVia marshals and unmarshals s through encoding/json and restores a new
// digester from the decoded copy.
func restoreVia(t *testing.T, cfg Config[note], snap Snapshot[note]) *Digester[note] {
	t.Helper()
	raw, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	var s Snapshot[note]
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	rest, err := Restore(cfg, s)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	return rest
}

func sameBatches(t *testing.T, a, b []Batch[note]) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("batch count %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Key != b[i].Key || a[i].Reason != b[i].Reason ||
			!a[i].OpenedAt.Equal(b[i].OpenedAt) || !a[i].DueAt.Equal(b[i].DueAt) ||
			!eq(texts(a[i]), texts(b[i])) {
			t.Fatalf("batch %d differs: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	cfg := Config[note]{Key: key, Window: 60 * time.Second, Dedup: idOf}

	t.Run("Given a mid-window Fixed digester with a collapsed duplicate", func(t *testing.T) {
		orig := mustNew(t, cfg)
		orig.Add(at(0), note{To: "r1", ID: "1", Text: "r1a"})
		orig.Add(at(2*time.Second), note{To: "r2", ID: "9", Text: "r2a"})
		orig.Add(at(3*time.Second), note{To: "r1", ID: "1", Text: "dup"}) // collapses
		orig.Add(at(4*time.Second), note{To: "r1", ID: "2", Text: "r1b"})
		snap := orig.Snapshot() // captured mid-window, before any drain

		t.Run("When it is snapshotted, JSON round-tripped and restored", func(t *testing.T) {
			rest := restoreVia(t, cfg, snap)
			t.Run("Then the restored digester drains identically", func(t *testing.T) {
				sameBatches(t, orig.Due(at(2*time.Minute)), rest.Due(at(2*time.Minute)))
			})
		})

		t.Run("When a further duplicate is offered to a restored copy", func(t *testing.T) {
			rest := restoreVia(t, cfg, snap)
			imm, fl := rest.Add(at(5*time.Second), note{To: "r1", ID: "1", Text: "dup2"})
			t.Run("Then the rebuilt dedup set still collapses it", func(t *testing.T) {
				if imm != nil || fl != nil {
					t.Fatalf("Add returned (%v,%v), want a collapsed duplicate", imm, fl)
				}
				got := rest.Due(at(2 * time.Minute))
				if len(got) != 2 {
					t.Fatalf("got %d batches, want 2", len(got))
				}
				// r1 kept r1a and r1b only (id 1 seen once, id 2 once).
				if !eq(texts(got[0]), []string{"r1a", "r1b"}) {
					t.Fatalf("r1 events = %v, want [r1a r1b]", texts(got[0]))
				}
			})
		})
	})
}

func TestSnapshotEmpty(t *testing.T) {
	cfg := Config[note]{Key: key, Window: time.Minute}
	t.Run("Given an empty digester", func(t *testing.T) {
		orig := mustNew(t, cfg)
		t.Run("When snapshotted and restored", func(t *testing.T) {
			s := orig.Snapshot()
			if len(s.Windows) != 0 || len(s.LastSend) != 0 {
				t.Fatalf("empty snapshot carried state: %+v", s)
			}
			rest := roundTrip(t, cfg, orig)
			t.Run("Then the restored digester is usable and empty", func(t *testing.T) {
				if p := rest.Pending(); len(p) != 0 {
					t.Fatalf("Pending = %v, want empty", p)
				}
				if _, fl := rest.Add(at(0), note{To: "r1", Text: "a"}); fl != nil {
					t.Fatalf("Add flushed unexpectedly")
				}
				if got := rest.Due(at(time.Minute)); len(got) != 1 {
					t.Fatalf("restored digester did not resume: %v", got)
				}
			})
		})
	})
}

func TestSnapshotBackoffMarkers(t *testing.T) {
	cfg := Config[note]{Key: key, Window: time.Minute, Strategy: Backoff, Lookback: 5 * time.Minute}
	t.Run("Given a Backoff digester with recent send markers for two keys", func(t *testing.T) {
		orig := mustNew(t, cfg)
		orig.Add(at(0), note{To: "r2", Text: "first"}) // passthrough, send@0
		imm, _ := orig.Add(at(0), note{To: "r1", Text: "first"})
		if len(imm) != 1 {
			t.Fatalf("setup passthrough failed: %v", imm)
		}
		t.Run("When snapshotted, the markers serialize sorted by key", func(t *testing.T) {
			s := orig.Snapshot()
			if len(s.LastSend) != 2 || s.LastSend[0].Key != "r1" || s.LastSend[1].Key != "r2" {
				t.Fatalf("LastSend = %v, want r1 then r2", s.LastSend)
			}
		})
		t.Run("When restored and a repeat arrives within Lookback", func(t *testing.T) {
			rest := roundTrip(t, cfg, orig)
			imm, fl := rest.Add(at(30*time.Second), note{To: "r1", Text: "second"})
			t.Run("Then the marker survived and the repeat opens a window", func(t *testing.T) {
				if imm != nil || fl != nil {
					t.Fatalf("Add returned (%v,%v), want a buffered repeat (marker restored)", imm, fl)
				}
				if p := rest.Pending(); len(p) != 1 {
					t.Fatalf("Pending = %v, want one window", p)
				}
			})
		})
	})
}

func TestSnapshotClockSurvives(t *testing.T) {
	cfg := Config[note]{Key: key, Window: time.Minute}
	t.Run("Given a digester whose clock has advanced to t=100s", func(t *testing.T) {
		orig := mustNew(t, cfg)
		orig.Due(at(100 * time.Second)) // advance clock without opening a window
		t.Run("When restored and offered an event with an earlier now", func(t *testing.T) {
			rest := roundTrip(t, cfg, orig)
			rest.Add(at(10*time.Second), note{To: "r1", Text: "a"})
			t.Run("Then the clock survived and the now is clamped forward", func(t *testing.T) {
				p := rest.Pending()
				if len(p) != 1 || !p[0].OpenedAt.Equal(at(100*time.Second)) {
					t.Fatalf("window opened at %v, want clamped to 100s", p)
				}
			})
		})
	})
}

func TestSnapshotIsACopy(t *testing.T) {
	cfg := Config[note]{Key: key, Window: time.Minute}
	t.Run("Given a snapshot taken mid-window", func(t *testing.T) {
		d := mustNew(t, cfg)
		d.Add(at(0), note{To: "r1", Text: "a"})
		s := d.Snapshot()
		t.Run("When the digester keeps running after the snapshot", func(t *testing.T) {
			d.Add(at(1*time.Second), note{To: "r1", Text: "b"})
			t.Run("Then the snapshot is an unchanged point-in-time copy", func(t *testing.T) {
				if len(s.Windows) != 1 || len(s.Windows[0].Events) != 1 {
					t.Fatalf("snapshot window mutated: %+v", s.Windows)
				}
			})
		})
	})
}

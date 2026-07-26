package digestr

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// FuzzNoLossNoPanic drives arbitrary Add/Due sequences (with jittered,
// sometimes-backward clocks) and asserts the core safety properties with dedup
// off: the engine never panics, and after a final drain every added event
// appears in exactly one place — a Backoff passthrough or exactly one flushed
// batch — with nothing lost, nothing duplicated, and nothing left pending.
func FuzzNoLossNoPanic(f *testing.F) {
	f.Add([]byte{0x01, 0x03, 0x02, 0x05, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})
	f.Add([]byte{0xff, 0xaa, 0x55, 0x0f, 0xf0, 0x3c, 0xc3})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 4 {
			t.Skip()
		}
		unit := time.Second
		cfg := Config[note]{
			Key:      key,
			Window:   time.Duration(1+data[1]%8) * unit,
			Strategy: Strategy(data[0] & 1),
			MaxSize:  int(data[2] % 4), // 0..3
			Lookback: time.Duration(1+data[3]%8) * unit,
		}
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("valid config rejected: %v", err)
		}

		seen := map[int]int{} // tag -> times observed in immediate/batched
		var added, immediate, batched int
		now := t0
		tag := 0

		record := func(e note) {
			seen[e.Tag]++
		}

		for _, b := range data[4:] {
			deltaSec := int(b & 0x7)
			now = now.Add(time.Duration(deltaSec) * unit)
			callNow := now
			if b&0x40 != 0 { // sometimes hand the engine a stale, backward now
				callNow = now.Add(-time.Duration(int(b&7)) * unit)
			}
			k := string(rune('a' + int((b>>3)%3)))
			switch (b >> 5) & 0x3 {
			case 3:
				for _, batch := range d.Due(callNow) {
					if len(batch.Events) == 0 {
						t.Fatalf("flushed an empty batch")
					}
					for _, e := range batch.Events {
						record(e)
						batched++
					}
				}
			default:
				imm, fl := d.Add(callNow, note{To: k, Tag: tag})
				tag++
				added++
				if imm != nil && fl != nil {
					t.Fatalf("Add returned both immediate and flushed: %v / %v", imm, fl)
				}
				for _, e := range imm {
					record(e)
					immediate++
				}
				if fl != nil {
					if fl.Reason != SizeReached {
						t.Fatalf("Add flush reason = %v, want SizeReached", fl.Reason)
					}
					for _, e := range fl.Events {
						record(e)
						batched++
					}
				}
			}
		}

		// Drain everything, then confirm draining is idempotent.
		far := now.Add(1 << 40)
		for _, batch := range d.Due(far) {
			for _, e := range batch.Events {
				record(e)
				batched++
			}
		}
		if left := d.Due(far); len(left) != 0 {
			t.Fatalf("second drain returned %d batches, want 0 (idempotent)", len(left))
		}
		if p := d.Pending(); len(p) != 0 {
			t.Fatalf("Pending after drain = %v, want empty", p)
		}

		// With no dedup, every added event is accounted for exactly once.
		if immediate+batched != added {
			t.Fatalf("accounting: immediate %d + batched %d != added %d", immediate, batched, added)
		}
		for tg := 0; tg < added; tg++ {
			if seen[tg] != 1 {
				t.Fatalf("tag %d observed %d times, want exactly 1", tg, seen[tg])
			}
		}
	})
}

// scriptedOp is one step in a randomized property run.
type scriptedOp struct {
	due bool
	now time.Time
	ev  note
}

// makeScript builds a deterministic op sequence from a seed: mixed Add/Due with
// three keys, four dedup ids (including the never-collapsing empty id), unique
// tags, and a mostly-forward clock that occasionally steps backward.
func makeScript(seed int64, n int) []scriptedOp {
	r := rand.New(rand.NewSource(seed))
	ids := []string{"", "x", "y", "z"}
	keys := []string{"a", "b", "c"}
	ops := make([]scriptedOp, 0, n)
	now := t0
	for i := 0; i < n; i++ {
		now = now.Add(time.Duration(r.Intn(6)) * time.Second)
		callNow := now
		if r.Intn(8) == 0 {
			callNow = now.Add(-time.Duration(r.Intn(4)) * time.Second)
		}
		if r.Intn(3) == 0 {
			ops = append(ops, scriptedOp{due: true, now: callNow})
			continue
		}
		ops = append(ops, scriptedOp{
			now: callNow,
			ev:  note{To: keys[r.Intn(len(keys))], ID: ids[r.Intn(len(ids))], Tag: i},
		})
	}
	return ops
}

// replay runs a script on a fresh digester and returns a transcript plus the
// dedup-aware invariant checks.
func replay(t *testing.T, cfg Config[note], ops []scriptedOp) []string {
	t.Helper()
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var transcript []string
	var added, immediate, batched int

	checkBatch := func(b Batch[note]) {
		ids := map[string]bool{}
		tags := make([]int, 0, len(b.Events))
		for _, e := range b.Events {
			if e.ID != "" {
				if ids[e.ID] {
					t.Fatalf("batch for %q carries duplicate id %q", b.Key, e.ID)
				}
				ids[e.ID] = true
			}
			tags = append(tags, e.Tag)
		}
		transcript = append(transcript, fmt.Sprintf("flush %s %v %s", b.Key, tags, b.Reason))
		batched += len(b.Events)
	}

	for _, op := range ops {
		if op.due {
			for _, b := range d.Due(op.now) {
				checkBatch(b)
			}
			continue
		}
		added++
		imm, fl := d.Add(op.now, op.ev)
		for _, e := range imm {
			transcript = append(transcript, fmt.Sprintf("imm %d", e.Tag))
			immediate++
		}
		if fl != nil {
			checkBatch(*fl)
		}
	}
	// Final drain.
	for _, b := range d.Due(t0.Add(1 << 40)) {
		checkBatch(b)
	}
	pending := 0
	for _, w := range d.Pending() {
		pending += w.Size
	}
	if pending != 0 {
		t.Fatalf("pending after final drain = %d, want 0", pending)
	}
	// Every added event was delivered immediately, batched, or dropped as a
	// duplicate — never lost, never created.
	if immediate+batched > added {
		t.Fatalf("created events: immediate %d + batched %d > added %d", immediate, batched, added)
	}
	return transcript
}

func TestPropertyRandomized(t *testing.T) {
	t.Run("Given random Add/Due scripts with dedup enabled", func(t *testing.T) {
		cfgs := []Config[note]{
			{Key: key, Window: 30 * time.Second, Dedup: idOf},
			{Key: key, Window: 20 * time.Second, MaxSize: 3, Dedup: idOf},
			{Key: key, Window: 25 * time.Second, Strategy: Backoff, Lookback: time.Minute, Dedup: idOf},
			{Key: key, Window: 15 * time.Second, Strategy: Backoff, Lookback: 40 * time.Second, MaxSize: 4, Dedup: idOf},
		}
		t.Run("When each is replayed twice", func(t *testing.T) {
			t.Run("Then invariants hold and the two runs are identical", func(t *testing.T) {
				for seed := int64(0); seed < 60; seed++ {
					ops := makeScript(seed, 250)
					cfg := cfgs[seed%int64(len(cfgs))]
					a := replay(t, cfg, ops)
					b := replay(t, cfg, ops)
					if len(a) != len(b) {
						t.Fatalf("seed %d: transcript lengths %d vs %d", seed, len(a), len(b))
					}
					for i := range a {
						if a[i] != b[i] {
							t.Fatalf("seed %d: nondeterministic at %d: %q vs %q", seed, i, a[i], b[i])
						}
					}
				}
			})
		})
	})
}

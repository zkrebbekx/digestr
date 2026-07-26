package digestr

import (
	"strconv"
	"testing"
	"time"
)

var (
	benchImmediate []note
	benchFlushed   *Batch[note]
	benchBatches   []Batch[note]
)

// BenchmarkAddHotKey measures Add on a single hot key. MaxSize bounds the window
// so the benchmark reaches a steady state — mostly the join path, with a size
// flush every MaxSize events — instead of growing one slice without limit.
func BenchmarkAddHotKey(b *testing.B) {
	d, _ := New(Config[note]{Key: key, Window: time.Hour, MaxSize: 100})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchImmediate, benchFlushed = d.Add(t0, note{To: "hot", Tag: i})
	}
}

// BenchmarkAddBackoffPassthrough measures the Backoff passthrough path, where
// each lone event for a fresh key is returned immediately and opens no window.
func BenchmarkAddBackoffPassthrough(b *testing.B) {
	d, _ := New(Config[note]{Key: key, Window: time.Minute, Strategy: Backoff, Lookback: time.Nanosecond})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Distinct keys and Lookback of 1ns keep every event a lone passthrough.
		benchImmediate, benchFlushed = d.Add(t0, note{To: strconv.Itoa(i), Tag: i})
	}
}

// BenchmarkDueManyWindows measures Due flushing many simultaneously-due windows.
// Repopulation is excluded from the timer so only the flush-and-sort cost is
// measured.
func BenchmarkDueManyWindows(b *testing.B) {
	const windows = 1000
	d, _ := New(Config[note]{Key: key, Window: time.Minute})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for w := 0; w < windows; w++ {
			d.Add(t0, note{To: strconv.Itoa(w), Tag: w})
		}
		b.StartTimer()
		benchBatches = d.Due(t0.Add(2 * time.Minute))
	}
}

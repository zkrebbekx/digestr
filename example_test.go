package digestr_test

import (
	"fmt"
	"time"

	"github.com/zkrebbekx/digestr"
)

// Event is a caller's notification payload. digestr never inspects it beyond
// the key and dedup id the caller derives from it.
type Event struct {
	Recipient string
	ID        string
	Text      string
}

func recipient(e Event) string { return e.Recipient }

var base = time.Date(2026, time.January, 1, 9, 0, 0, 0, time.UTC)

// Example digests three activities for one recipient into a single batch when
// the window elapses.
func Example() {
	d, _ := digestr.New(digestr.Config[Event]{
		Key:    recipient,
		Window: time.Minute,
	})

	d.Add(base, Event{Recipient: "amy", Text: "liked your post"})
	d.Add(base.Add(10*time.Second), Event{Recipient: "amy", Text: "commented"})
	d.Add(base.Add(20*time.Second), Event{Recipient: "amy", Text: "followed you"})

	for _, b := range d.Due(base.Add(time.Minute)) {
		fmt.Printf("%s: %d updates (%s)\n", b.Key, len(b.Events), b.Reason)
	}
	// Output: amy: 3 updates (Elapsed)
}

// Example_sizeTrigger flushes early once a window reaches MaxSize, without
// waiting for the (here, one-hour) time trigger.
func Example_sizeTrigger() {
	d, _ := digestr.New(digestr.Config[Event]{
		Key:     recipient,
		Window:  time.Hour,
		MaxSize: 3,
	})

	for i, txt := range []string{"one", "two", "three"} {
		_, flushed := d.Add(base.Add(time.Duration(i)*time.Second), Event{Recipient: "amy", Text: txt})
		if flushed != nil {
			fmt.Printf("flushed %d on %s\n", len(flushed.Events), flushed.Reason)
		}
	}
	// Output: flushed 3 on SizeReached
}

// Example_backoff delivers a lone event immediately and only digests once
// events repeat within the look-back horizon.
func Example_backoff() {
	d, _ := digestr.New(digestr.Config[Event]{
		Key:      recipient,
		Window:   time.Minute,
		Strategy: digestr.Backoff,
		Lookback: 5 * time.Minute,
	})

	imm, _ := d.Add(base, Event{Recipient: "amy", Text: "first"})
	fmt.Printf("lone event delivered now: %d\n", len(imm))

	repeat, _ := d.Add(base.Add(30*time.Second), Event{Recipient: "amy", Text: "repeat"})
	fmt.Printf("repeat delivered now: %d (it is digesting instead)\n", len(repeat))
	// Output:
	// lone event delivered now: 1
	// repeat delivered now: 0 (it is digesting instead)
}

// Example_dedup collapses events that share a non-empty id within one window.
func Example_dedup() {
	d, _ := digestr.New(digestr.Config[Event]{
		Key:    recipient,
		Window: time.Minute,
		Dedup:  func(e Event) string { return e.ID },
	})

	d.Add(base, Event{Recipient: "amy", ID: "order-42", Text: "shipped"})
	d.Add(base.Add(time.Second), Event{Recipient: "amy", ID: "order-42", Text: "shipped"})
	d.Add(base.Add(2*time.Second), Event{Recipient: "amy", ID: "order-43", Text: "delivered"})

	for _, b := range d.Due(base.Add(time.Minute)) {
		fmt.Printf("%d distinct events\n", len(b.Events))
	}
	// Output: 2 distinct events
}

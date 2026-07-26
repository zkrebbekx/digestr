// Command examples wraps the pure digestr core with a real wall-clock ticker.
// The library owns no timer or goroutine — the caller drives the clock — so
// this program supplies one: a ticker calls Due on a cadence while a producer
// goroutine feeds events concurrently, relying on the Digester's internal lock.
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/zkrebbekx/digestr"
)

// Activity is a social-feed event: someone did something to a recipient.
type Activity struct {
	Recipient string
	ID        string
	Verb      string
}

func main() {
	d, err := digestr.New(digestr.Config[Activity]{
		Key:     func(a Activity) string { return a.Recipient },
		Window:  200 * time.Millisecond,
		MaxSize: 4, // a hot recipient flushes early instead of waiting the window
		Dedup:   func(a Activity) string { return a.ID },
	})
	if err != nil {
		panic(err)
	}

	deliver := func(reason, key string, evs []Activity) {
		verbs := make([]string, len(evs))
		for i, e := range evs {
			verbs[i] = e.Verb
		}
		fmt.Printf("[%-11s] %-3s digest of %d: %v\n", reason, key, len(verbs), verbs)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Producer: a burst of activities, concurrent with the ticker below.
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(1))
		recipients := []string{"amy", "ben"}
		verbs := []string{"liked", "commented", "followed", "mentioned", "shared"}
		for i := 0; i < 24; i++ {
			a := Activity{
				Recipient: recipients[r.Intn(len(recipients))],
				ID:        fmt.Sprintf("act-%d", i),
				Verb:      verbs[r.Intn(len(verbs))],
			}
			// immediate fires only under the Backoff strategy; flushed fires
			// when this Add fills a window to MaxSize.
			if immediate, flushed := d.Add(time.Now(), a); flushed != nil {
				deliver(flushed.Reason.String(), flushed.Key, flushed.Events)
				_ = immediate
			}
			time.Sleep(25 * time.Millisecond)
		}
		close(stop)
	}()

	// Consumer: the wall clock the pure core deliberately lacks.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for draining := true; draining; {
		select {
		case <-ticker.C:
			for _, b := range d.Due(time.Now()) {
				deliver(b.Reason.String(), b.Key, b.Events)
			}
		case <-stop:
			// Final drain, well past every open window.
			for _, b := range d.Due(time.Now().Add(time.Hour)) {
				deliver(b.Reason.String(), b.Key, b.Events)
			}
			draining = false
		}
	}
	wg.Wait()

	fmt.Printf("pending after drain: %d\n", len(d.Pending()))
}

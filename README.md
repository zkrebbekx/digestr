# digestr

A pure-Go **notification digest / batching engine**: many events per recipient
in, one batched notification out, on a window you control. Zero dependencies.

Every notification product eventually needs "email me a digest of the 12 things
that happened, not 12 emails." The category leaders sell that as a hosted
workflow step — six containers, MongoDB *and* Redis, the digest computed on
their servers. Their Go artifacts are generated REST clients: `Trigger` and
`Cancel`, nothing local. General-purpose Go debouncers don't close the gap
either — their callbacks are `func()` with no payload and no key, so they
collapse repeated calls last-wins but cannot *accumulate N events per recipient
into a batch*. Single-value debounce is not a digest.

`digestr` is just the batching core, extracted: it does the grouping-and-
windowing computation and *only* that. It never sends, stores, templates, or
owns a channel — which is exactly why it needs no database, no broker, and no
running process.

- **Pure and clock-driven.** No goroutines, no timers. The caller advances time
  by passing `now` to `Add` and `Due`, so the engine is deterministic, trivial
  to test, and safe to persist.
- **Two triggers.** A window flushes on **time** (it elapsed) or **size** (a
  count ceiling — Knock's `batch_size`, which bounds latency for a hot key),
  whichever comes first.
- **Two strategies.** *Fixed* digests every event; *Backoff* passes a lone event
  straight through and only digests when events **repeat** within a look-back
  horizon (Novu's "when events repeat").
- **Dedup** within a window by a caller-supplied idempotency id.
- **Deterministic** flush ordering — same construction, same `now`, identical
  output every run.
- **Serializable state.** Snapshot the open windows, persist them however you
  like, and restore to resume exactly across a restart.
- **Concurrency-safe**, race-clean, generic over your event type.
- **Zero dependencies.** Go 1.23+.

```go
import "github.com/zkrebbekx/digestr"
```

## Example

```go
d, _ := digestr.New(digestr.Config[Event]{
    Key:    func(e Event) string { return e.Recipient },
    Window: time.Minute,
})

d.Add(t0, Event{Recipient: "amy", Text: "liked your post"})
d.Add(t0.Add(10*time.Second), Event{Recipient: "amy", Text: "commented"})
d.Add(t0.Add(20*time.Second), Event{Recipient: "amy", Text: "followed you"})

for _, b := range d.Due(t0.Add(time.Minute)) {
    send(b.Key, b.Events) // one digest of three events, Reason Elapsed
}
```

## The model — a clock-driven state machine

`digestr` holds one open **window** per active key. The first event for a key
opens a window `[T, T+Window)`; later events for that key join it. A window
flushes exactly once, into a `Batch`, on whichever trigger fires first:

- **Time.** `Due(now)` flushes every window whose `DueAt` is at or before
  `now` — Reason `Elapsed`. The boundary is inclusive: a window opened at `T`
  is due at exactly `T+Window`. `Due` closes what it flushes, sorts the batches
  by key, and is idempotent (a second `Due(now)` returns nothing).
- **Size.** If `MaxSize > 0` and a window reaches that many distinct events, the
  `Add` that fills it flushes it immediately — Reason `SizeReached`, before the
  time trigger. A later `Due` does not re-emit those events.

```go
type Config[E any] struct {
    Key      func(E) string // grouping key; required
    Window   time.Duration  // how long a window stays open; required, > 0
    Strategy Strategy       // Fixed (default) or Backoff
    MaxSize  int            // 0 = no size trigger; N>0 flushes early at N
    Lookback time.Duration  // Backoff only: the "recent send" horizon
    Dedup    func(E) string // optional; "" id is never deduplicated
}
```

## The `Add` contract

`Add(now, e)` returns `(immediate []E, flushed *Batch[E])` and sets **at most
one** of them:

| Result | Meaning |
| --- | --- |
| `(nil, nil)` | The event joined an open window, or was dropped as a duplicate. The common case. |
| `([e], nil)` | A **Backoff passthrough**: no window, deliver `e` now. |
| `(nil, &batch)` | This `Add` filled a window to `MaxSize`; deliver `batch` now. |

They are never both set: a passthrough opens no window to flush, and a size
flush requires an open window that a passthrough would not have created. `Add`
never time-flushes — that is `Due`'s job.

## Strategies

**Fixed** (Novu "Regular") digests every event: the first opens a window and the
rest join.

**Backoff** (Novu "when events repeat") digests only repeats. A lone event for a
key with no recent send passes straight through, returned as `immediate`, and
opens no window; only a follow-up **within `Lookback`** opens a digest. Both a
passthrough delivery *and* a batch flush count as a "send" that starts the
look-back horizon, so a cold key's second event within `Lookback` digests — see
the correction note in [`docs/DESIGN.md`](docs/DESIGN.md). The recency horizon
is half-open: a send exactly `Lookback` ago is no longer recent.

```go
d, _ := digestr.New(digestr.Config[Event]{
    Key: recipient, Window: time.Minute,
    Strategy: digestr.Backoff, Lookback: 5 * time.Minute,
})
imm, _ := d.Add(t0, Event{Recipient: "amy", Text: "first"})       // len(imm)==1: delivered now
imm2, _ := d.Add(t0.Add(30*time.Second), Event{Recipient: "amy"}) // len(imm2)==0: digesting
```

## Dedup

A non-nil `Dedup` collapses duplicates within a single window: an event whose
non-empty id a still-open window has already seen is dropped, and the batch
keeps the one copy. An empty (`""`) id is never deduplicated, and dedup never
spans windows.

## Out-of-order time

The caller drives the clock, so `digestr` defends against a non-monotonic one.
It tracks the highest `now` it has observed; any earlier `now` handed to `Add`
or `Due` is **clamped forward** to it. Windows therefore never carry a negative
span, `Due` never un-fires, and no event is lost. Call `Due` on a cadence no
coarser than your smallest `Window`; between `Due` calls an overdue window keeps
accepting joins (it flushes lazily on the next `Due`).

## Persistence — the crash-safety boundary

An open window is state: if the process dies, un-flushed events must not be lost
or double-sent. `digestr` keeps that state in memory and makes it *yours* to
persist, rather than dragging in a database.

```go
snap := d.Snapshot()               // serializable open windows + markers + clock
blob, _ := json.Marshal(snap)      // persist however you like
// ... restart ...
var s digestr.Snapshot[Event]
json.Unmarshal(blob, &s)
d, _ := digestr.Restore(cfg, s)    // resumes exactly; Config supplied fresh
```

Only *data* persists — never the `Config`'s function fields, which you supply
afresh to `Restore`. For a `Snapshot[E]` to survive a JSON round-trip, your
event type `E` must have exported fields, as with any `encoding/json` value.

## Concurrency

A `*Digester` is safe for concurrent `Add` and `Due`; every method takes an
internal mutex, and `Snapshot` is a consistent point-in-time copy. A notification
hot path is inherently concurrent, so this is the default, not an option.

## Errors

`New` and `Restore` return typed sentinels matchable with `errors.Is`:
`ErrNoKey`, `ErrBadWindow`, `ErrBadSize`, `ErrBadLookback` and `ErrBadStrategy`.
A `MaxSize` of zero is valid (no size trigger); a negative one is an error.

## Non-goals

- **No delivery.** `digestr` tells you *what to send and when*; the channel is
  yours. This is why it needs no SMTP, no push gateway, no broker.
- **No templating, channels, or preferences.** Rendering the batch and picking
  email vs push are the caller's.
- **No storage or running process.** `digestr` is a computation, not a daemon.
  It owns no goroutines and no timers; crash-safe persistence is delegated to
  the caller via snapshot/restore.
- **No quiet hours.** A scheduling concern the caller composes on top.

## License

MIT

# digestr — design

A pure-Go **notification digest / batching engine**: many events per recipient
in, one batched notification out, on a window you control. Zero dependencies.

digestr does the grouping-and-windowing computation that Novu and Knock
monetize as a workflow step — and it does *only* that. It does not send, store,
template, or own channels. It is the batching core, extracted.

Status: design. Not yet implemented.

## Why this exists

Verified 2026-07-26 against the live GitHub API and vendor docs: there is no
maintained, embeddable, low-infra Go library that computes per-recipient digest
windows. The category leaders are services:

- **Novu** (39k★) — a TypeScript platform: six containers, MongoDB **and** Redis
  hard-required; digest runs on their servers. Its Go artifact (`novu-go`) is a
  generated HTTP client — `Trigger`/`Cancel`, nothing local.
- **Knock** — SaaS; the only Go it ships is a Stainless-generated REST client.
  No self-hostable engine.
- General-purpose Go debouncers (`bep/debounce`, `godebouncer`, `cable`) do
  **not** close the gap: their callbacks are `func()` with no payload and no
  key, so they collapse repeated calls last-wins but cannot *accumulate N events
  per recipient into a batch*. Single-value debounce is not a digest.
- `readraptor`, the one Go notification-digest project, is an abandoned (2015)
  Postgres service that owns delivery.

So a Go service that wants "email me a digest of the 12 things that happened,
not 12 emails" has no library to reach for. That is digestr.

## Scope

**In scope — the whole library:**
- Group events by a caller-defined key (recipient, or recipient + sub-key).
- Open a window on the first event for a key.
- Flush that window on either trigger: **time** (the window elapsed) or **size**
  (a count ceiling reached — Knock's `batch_size`, which bounds latency).
- **Backoff / look-back**: a lone event with no recent flush for its key passes
  straight through, undigested; only a *repeat* within the look-back window
  opens a digest (Novu's "when events repeat").
- **Dedup** within a window by a caller-supplied idempotency id.
- **Deterministic** flush ordering.
- Serializable pending state, so a caller can persist it and survive a restart.

**Explicitly NOT in scope — the exclusions keep digestr embeddable and pure:**
- **Delivery.** digestr never sends anything. It tells you *what to send and
  when*; the channel is yours. (This is why it needs no MongoDB, no Redis, no
  SMTP — the entire reason the incumbents are heavy.)
- **Templating / channels / preferences.** Rendering the batch, picking email
  vs push, per-user channel preferences — all the caller's. (Preference
  resolution is a different, disjoint capability.)
- **Storage / a running process.** digestr is a computation, not a daemon. It
  owns no goroutines and no timers; the caller drives the clock. Crash-safe
  persistence is delegated (see *Persistence*).

## The model — a pure, clock-driven state machine

digestr holds one open **window** per active key. It is not a goroutine and
starts no timers: the caller advances time and drains what is due, which makes
it deterministic, trivially testable, and safe to persist.

```go
// E is the caller's event payload — digestr is generic over it and never
// inspects it beyond the key and (optional) dedup id the caller supplies.
type Digester[E any] struct{ /* unexported: config + open windows */ }

type Config[E any] struct {
    Key    func(E) string        // grouping key; required
    Window time.Duration         // how long a window stays open
    Strategy Strategy            // Fixed or Backoff
    MaxSize int                  // 0 = no size trigger; >0 = flush early at N (Knock batch_size)
    Lookback time.Duration       // Backoff only: "recent" horizon for the passthrough rule
    Dedup   func(E) string       // optional; empty/"" id means no dedup for that event
}

func New[E any](cfg Config[E]) (*Digester[E], error)

// Add offers an event at instant now. It returns any events that should be
// delivered IMMEDIATELY (a Backoff lone-event passthrough), and any Batch that
// this event's arrival caused to flush right now (a size-triggered flush).
func (d *Digester[E]) Add(now time.Time, e E) (immediate []E, flushed *Batch[E])

// Due returns every window whose time trigger has fired at or before now, in
// deterministic order, and closes them. Call it on whatever cadence you like
// (a ticker, a cron, lazily before a read); the result is the same for the
// same now.
func (d *Digester[E]) Due(now time.Time) []Batch[E]

// Pending reports keys with an open window and when each is due — for
// observability and for deciding when next to call Due.
func (d *Digester[E]) Pending() []Window
```

```go
type Batch[E any] struct {
    Key     string
    Events  []E       // in arrival order, deduped
    OpenedAt, DueAt time.Time
    Reason  FlushReason  // Elapsed or SizeReached
}
```

### Fixed strategy (Novu Regular)
The first event for a key opens a window `[now, now+Window)`. Subsequent events
for that key join it. The window flushes once, when `Due(t)` is called with
`t >= DueAt`, or early via `Add` if `MaxSize` is reached. One flush per window;
the aggregated events are the batch.

> Golden (Novu): "Events are grouped per subscriber… Once the digest window
> completes, the workflow continues once, with the aggregated events available
> as context."

### Backoff strategy (Novu "when events repeat")
On `Add`, if there was **no** flush for this key within the last `Lookback`,
the event is returned as an immediate passthrough and no window opens — a lone
event is not digested. If there *was* a recent flush (a repeat), a window opens
and events digest as in Fixed. This needs digestr to remember, per key, the
instant of the last flush.

> Golden (Novu): "Check whether a similar notification was sent recently. If
> yes, start a digest. If not, deliver the notification immediately and skip
> digesting." Presets 5 min / 30 min / custom.

### Size trigger (Knock `batch_size`)
Independent of strategy: if `MaxSize > 0` and a window reaches `MaxSize` events,
`Add` flushes it immediately and returns the batch, without waiting for the
time trigger. This bounds notification latency for a hot key.

> Golden (Knock): a batch groups on `concat(recipient_id, batch_key)`,
> accumulates `activities`, and `batch_size` is a count ceiling that flushes
> early.

## Golden test vectors (these become the suite)

1. **Fixed, time flush.** Window 60s. Three events for `r1` at t=0,10,20 →
   `Due(t=59)` empty; `Due(t=60)` returns one batch of 3, reason Elapsed.
2. **Fixed, immediate open.** First event opens the window at its own instant;
   the DueAt is firstEvent+Window, not a wall-clock grid.
3. **Size trigger.** Window 1h, MaxSize 5. Five events for `r1` → the 5th
   `Add` returns a batch of 5, reason SizeReached, before the hour; a later
   `Due` does not re-emit them.
4. **Backoff passthrough.** No recent flush for `r1` → first `Add` returns the
   event as `immediate`, opens no window.
5. **Backoff repeat.** A flush for `r1` at t=0; an `Add` at t < Lookback opens a
   digest window instead of passing through.
6. **Dedup.** Same dedup id twice in a window → the batch carries one copy.
7. **Grouping independence.** Events for `r1` and `r2` flush as separate
   batches on their own windows.
8. **Determinism.** `Due(t)` over identical construction returns the same
   batches in the same order every run (keys sorted, events in arrival order).

## Persistence — the crash-safety boundary

An open window is state: if the process dies, un-flushed events must not be
lost or double-sent. digestr keeps that state in memory and makes it the
caller's to persist, rather than dragging in a database — that delegation is
what keeps it a zero-dependency library instead of Novu's MongoDB.

```go
func (d *Digester[E]) Snapshot() Snapshot[E]     // serializable open windows + last-flush times
func Restore[E any](cfg Config[E], s Snapshot[E]) (*Digester[E], error)
```

A caller persists the snapshot on shutdown (and/or periodically) and restores on
boot; the window state machine resumes exactly. `Snapshot[E]` is a plain
exported struct the caller can marshal with encoding/json or anything else. A
`Store`-interface variant that persists per-window on every change is possible
later; v0.1 keeps the boundary at snapshot/restore, which is honest about the
durability the caller is choosing.

## Non-goals restated

No delivery, no channels, no templates, no preferences, no quiet-hours (a
scheduling concern the caller composes on top), no goroutines or timers inside
the core, no database. digestr computes batches; everything else is the
caller's.

## Open questions

1. Should `Due` also return backoff windows that never repeated (i.e. a window
   opened by a repeat but then quiet)? Yes — a time flush applies to any open
   window regardless of how it opened.
2. Concurrency: is `*Digester` safe for concurrent `Add`/`Due`? Lean: guard it
   with a mutex internally (a notification hot-path is inherently concurrent),
   and document that `Snapshot` is a consistent point-in-time copy.
3. Deterministic ordering across keys: sort flushed batches by key, then by
   DueAt, so a multi-key `Due` is reproducible.

## Phase 1 — as built

Implemented as a zero-dependency library (`Digester[E]`, `Config[E]`, `New`,
`Add`, `Due`, `Pending`, `Batch[E]`, `Window`, `Strategy`, `FlushReason`,
`Snapshot[E]`, `Restore`), 100% statement coverage, stdlib-only tests. Where the
sketch above left a choice open, or was wrong, the build resolved it as follows.

### Correction 1 — Backoff keys off the last *send*, not the last *flush*

The sketch says Backoff should "remember, per key, the instant of the last
**flush**." Taken literally that makes the strategy inert: the very first event
for a key always passes through (it is not a repeat) and, under a flush-only
marker, records *nothing* — so the second lone event also sees "no recent flush"
and passes through too, and so on forever. Nothing ever bootstraps into a
digest.

Novu's own rule is stated over sends, not flushes: "check whether a similar
notification was **sent** recently… if not, **deliver** immediately." An
immediate passthrough delivery *is* a send. So digestr records the last **send**
per key, and **both** a Backoff passthrough and a batch flush update it.
Consequences:

- A cold key's first event passes through and marks a send; a repeat within
  `Lookback` then opens a digest — the intended behaviour, now reachable.
- A flush (time or size) also marks a send, so the repeat chain continues after
  each digest closes.
- Golden vector 5 ("a flush for r1 at t=0 → an Add within Lookback opens a
  window") is reproduced faithfully; the seed send at t=0 can be either a
  passthrough or a real flush — the tests cover both.

The marker is maintained **only** under Backoff (Fixed keeps none, avoiding an
unbounded per-key map), and `Due` prunes markers older than `Lookback`, which
can no longer affect any decision — a behaviour-neutral bound on live memory.

### Correction 2 — the Backoff look-back boundary is half-open

At exactly `lastSend + Lookback` the prior send has **aged out**: recency is
`now - lastSend < Lookback` (strict), so an event exactly `Lookback` later
**passes through**, and one a nanosecond earlier digests. This matches the
sketch's own golden-5 phrasing ("an Add at `t < Lookback` opens a window") and
mirrors the window's half-open occupancy. (The *time-flush* boundary is the
opposite — inclusive: `Due` flushes when `now >= DueAt`, so a 60s window flushes
at exactly t+60, not t+59. The two boundaries answer different questions and are
tested at the edge on both sides.)

### Correction 3 — the `Add` return contract has three outcomes, never four

`Add` sets **at most one** of `(immediate, flushed)`:

- `(nil, nil)` — the event joined an open window **or** was dropped as a
  duplicate. This is the common case and the one the sketch omitted.
- `([e], nil)` — a Backoff passthrough (no window opened).
- `(nil, &batch)` — this `Add` filled a window to `MaxSize` (Reason
  `SizeReached`).

Both are never set together: a passthrough opens no window to flush, and a size
flush needs an open window a passthrough would not have created. `Add` **never
time-flushes** — `flushed` is always `SizeReached`, never `Elapsed`; the time
trigger belongs to `Due`. A consequence: a window already past its `DueAt` but
not yet drained keeps accepting joins (lazily) until the next `Due`, rather than
`Add` closing it early. Call `Due` on a cadence no coarser than the smallest
`Window` to bound this.

### Correction 4 — out-of-order time is clamped forward, not rejected

`Add` has no error return, and time is caller-supplied, so digestr defends
against a non-monotonic clock by tracking the highest `now` it has observed and
**clamping** any earlier `now` forward to it. This guarantees windows never
carry a negative span, `Due` never un-fires (so it stays idempotent for a fixed
`now`), and no event is ever lost or duplicated. The clock is part of the
snapshot, so monotonicity survives a restart.

### Smaller decisions

- **Ordering is by key alone.** At most one window is open per key, so flushed
  batches (and `Pending`, and snapshot windows) always have distinct keys, which
  makes the "then DueAt" tie-break in the sketch dead code; it was dropped.
- **`Window` gained a `Size` field** (current event count) for observability,
  beyond the sketch's `Key`/`OpenedAt`/`DueAt`.
- **The dedup set is not persisted.** `Restore` rebuilds each window's seen-id
  set by applying the fresh `Config.Dedup` to the surviving `Events` — which are
  exactly the distinct ids that were seen — so a duplicate offered after a
  restore still collapses. Snapshot slices are sorted by key so equal state
  serializes byte-for-byte alike.
- **A duplicate never triggers the size flush**: dedup runs before the size
  check, so `MaxSize` counts distinct events only.
- **`ErrBadStrategy`** was added to the four named sentinels, to reject a
  `Strategy` value that is neither `Fixed` nor `Backoff`.

### Deferred

- A `Store`-interface variant that persists per-window on every change (the
  sketch already flags this as post-v0.1); v0.1 keeps the boundary at
  snapshot/restore.
- Marker pruning is opportunistic (on `Due`). A caller that only ever uses
  size/passthrough and never calls `Due` under Backoff would retain markers for
  every key seen; a future version could add an explicit prune or a TTL.

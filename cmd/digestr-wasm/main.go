//go:build js && wasm

// Command digestr-wasm is the browser playground's engine: the digestr
// notification digest / batching core compiled to WebAssembly, exposing a small
// JSON API on the global "digestr" object. It has no backend — every call runs
// in the visitor's tab over the pure library, and nothing leaves the page.
//
// digestr is stateful and generic: a Digester[E] accumulates events per key
// until a window flushes. JavaScript cannot parameterize a Go generic across the
// boundary, so the bridge instantiates the digester with one concrete event
// type — the `event` struct below, decoded from JSON — and groups it by its
// Recipient. The bridge therefore holds a live session (a *digestr.Digester, its
// config, a virtual clock and the batches it has emitted) in a Go map keyed by a
// session id the page passes. A mutating call (addEvent, advanceClock, flush,
// reset) mutates the live digester and returns the whole fresh state, so the
// page re-renders from one value.
//
// The clock is the caller's. digestr owns no goroutine and no timer: the page
// advances time by passing a now to Add and Due. Instants cross the boundary as
// seconds from a fixed session base, so the page draws a clean "T+0s … T+90s"
// timeline. The clock is monotonic on purpose, mirroring digestr's own forward
// clamp: a now earlier than one already observed is pulled forward, so a batch
// once flushed never un-flushes.
//
// This file carries the js && wasm build constraint so the native toolchain —
// go build ./..., go vet ./..., go test ./..., golangci-lint — never tries to
// compile it (syscall/js exists only under GOOS=js). The root module stays
// zero-dependency: syscall/js is stdlib and adds nothing to go.sum, and this
// command imports only the digestr library beside it.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"syscall/js"
	"time"

	"github.com/zkrebbekx/digestr"
)

func main() {
	obj := map[string]any{}
	register(obj, "newSession", handleNewSession)
	register(obj, "addEvent", handleAddEvent)
	register(obj, "advanceClock", handleAdvanceClock)
	register(obj, "flush", handleFlush)
	register(obj, "reset", handleReset)
	register(obj, "state", handleState)
	register(obj, "loadScenario", handleLoadScenario)
	obj["scenarios"] = toJS(scenarioList())

	js.Global().Set("digestr", js.ValueOf(obj))
	js.Global().Set("__digestrReady", js.ValueOf(true))
	if cb := js.Global().Get("__digestrOnReady"); cb.Type() == js.TypeFunction {
		cb.Invoke()
	}

	// Keep the Go runtime alive for the lifetime of the page.
	select {}
}

// register wraps a handler so every call recovers from a panic into a returned
// {error} object rather than tearing down the wasm instance, and marshals the
// result to a real JS object the page can read without parsing.
func register(obj map[string]any, name string, fn func([]byte) (any, error)) {
	obj[name] = js.FuncOf(func(_ js.Value, args []js.Value) (result any) {
		defer func() {
			if r := recover(); r != nil {
				result = toJS(map[string]any{"error": fmt.Sprintf("panic: %v", r), "code": "panic"})
			}
		}()
		out, err := fn(input(args))
		if err != nil {
			return toJS(errObj(err))
		}
		return toJS(out)
	})
}

// input normalises the first argument to JSON bytes. A JS object is stringified;
// a bare string is passed through verbatim (loadScenario accepts one); anything
// absent becomes an empty object.
func input(args []js.Value) []byte {
	if len(args) == 0 {
		return []byte("{}")
	}
	a := args[0]
	switch a.Type() {
	case js.TypeString:
		return []byte(a.String())
	case js.TypeUndefined, js.TypeNull:
		return []byte("{}")
	default:
		return []byte(js.Global().Get("JSON").Call("stringify", a).String())
	}
}

// toJS marshals a Go value and parses it back into a native JS object so the page
// receives structured data rather than a string to parse itself.
func toJS(v any) js.Value {
	b, err := json.Marshal(v)
	if err != nil {
		b, _ = json.Marshal(map[string]any{"error": err.Error(), "code": "marshal"})
	}
	return js.Global().Get("JSON").Call("parse", string(b))
}

// --- error contract ---------------------------------------------------------

// errObj renders an error as the page's {error, code} shape.
func errObj(err error) map[string]any {
	return map[string]any{"error": err.Error(), "code": errCode(err)}
}

// argErr is a bridge-minted validation error (an unknown session, a bad enum, a
// value past a cap) distinct from a library sentinel. It carries a stable code.
type argErr struct {
	code string
	msg  string
}

func (e *argErr) Error() string { return e.msg }

func badArg(code, msg string) error { return &argErr{code: code, msg: msg} }

// errCode maps an error to a stable string code: a bridge argErr keeps its own
// code, and every digestr sentinel is mirrored onto its name. An unrecognised
// error is "error".
func errCode(err error) string {
	var a *argErr
	if errors.As(err, &a) {
		return a.code
	}
	switch {
	case errors.Is(err, digestr.ErrNoKey):
		return "no_key"
	case errors.Is(err, digestr.ErrBadWindow):
		return "bad_window"
	case errors.Is(err, digestr.ErrBadSize):
		return "bad_size"
	case errors.Is(err, digestr.ErrBadLookback):
		return "bad_lookback"
	case errors.Is(err, digestr.ErrBadStrategy):
		return "bad_strategy"
	case errors.Is(err, digestr.ErrBadSnapshot):
		return "bad_snapshot"
	default:
		return "error"
	}
}

// --- input bounds -----------------------------------------------------------

// These caps stop a hostile or accidental input from forcing an astronomical
// amount of work or memory in the tab. They are far larger than any story needs.
const (
	maxSessions        = 32
	maxEventsPerSess   = 4000
	maxWindowSeconds   = 100000
	maxLookbackSeconds = 100000
	maxSizeCap         = 2000
	maxStrLen          = 256
	maxAtSeconds       = 10000000
)

// sessionBase is the instant every session's clock starts at: T+0. Every wire
// instant is a whole/fractional second offset from it, so the page timeline
// reads as a plain "T+0s … T+90s" axis with no calendar noise.
var sessionBase = time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)

func atTime(sec float64) time.Time { return sessionBase.Add(time.Duration(sec * float64(time.Second))) }
func secOf(t time.Time) float64    { return t.Sub(sessionBase).Seconds() }

// --- the concrete event type the digester is instantiated over --------------

// event is the caller's notification payload. digestr is generic over it and
// never inspects it beyond the Recipient (the grouping Key) and, when dedup is
// on, the ID. Seq is assigned by the bridge so a chip keeps a stable identity as
// it moves from a pending window into an emitted batch. AtSec is the instant the
// event was offered, in seconds from the session base.
type event struct {
	Recipient string  `json:"recipient"`
	Kind      string  `json:"kind"`
	Body      string  `json:"body"`
	ID        string  `json:"id,omitempty"`
	Seq       int     `json:"seq"`
	AtSec     float64 `json:"atSec"`
}

// recipientKey is the digester's grouping key: events sharing a recipient digest
// together, exactly as Config.Key promises.
func recipientKey(e event) string { return e.Recipient }

// dedupID is the optional Config.Dedup: an empty id is never deduplicated.
func dedupID(e event) string { return e.ID }

// --- session state ----------------------------------------------------------

// emitted is one thing digestr told the caller to send: a flushed Batch (Reason
// Elapsed or SizeReached) or a Backoff passthrough (a lone event delivered
// immediately, no window). The bridge keeps them in emit order so the page can
// draw the "events in vs batches out" ledger.
type emitted struct {
	Seq        int
	Key        string
	Events     []event
	OpenedSec  float64
	DueSec     float64
	FlushedSec float64
	Reason     string // "Elapsed" | "SizeReached" | "Passthrough"
}

// session is one live digester and everything the page drives it with. The
// digester owns the open windows and the Backoff markers internally; the bridge
// keeps the config echo (to re-new on reset and to render), the virtual clock,
// the emitted deliveries and the recipient order seen so lanes keep a stable
// position.
type session struct {
	id         string
	d          *digestr.Digester[event]
	window     time.Duration
	maxSize    int
	strategy   digestr.Strategy
	lookback   time.Duration
	dedup      bool
	clock      time.Time
	eventsIn   int
	evSeq      int
	batchSeq   int
	emitted    []emitted
	recipOrder []string
	recipSeen  map[string]bool
}

var (
	mu       sync.Mutex
	sessions = map[string]*session{}
	seq      int
)

func getSession(id string) (*session, error) {
	mu.Lock()
	defer mu.Unlock()
	s, ok := sessions[id]
	if !ok {
		return nil, badArg("unknown_session", fmt.Sprintf("no live session %q; start one first, or reload", id))
	}
	return s, nil
}

// seeRecipient records a recipient in first-seen order so its lane keeps a fixed
// column position across renders.
func (s *session) seeRecipient(r string) {
	if s.recipSeen == nil {
		s.recipSeen = map[string]bool{}
	}
	if !s.recipSeen[r] {
		s.recipSeen[r] = true
		s.recipOrder = append(s.recipOrder, r)
	}
}

// recordBatch appends a flushed Batch as an emitted delivery.
func (s *session) recordBatch(b digestr.Batch[event], flushedAt time.Time) {
	s.batchSeq++
	s.emitted = append(s.emitted, emitted{
		Seq:        s.batchSeq,
		Key:        b.Key,
		Events:     b.Events,
		OpenedSec:  secOf(b.OpenedAt),
		DueSec:     secOf(b.DueAt),
		FlushedSec: secOf(flushedAt),
		Reason:     b.Reason.String(),
	})
}

// --- config -----------------------------------------------------------------

type configSpec struct {
	SessionID string  `json:"sessionId"`
	Strategy  string  `json:"strategy"`
	WindowSec float64 `json:"windowSec"`
	MaxSize   int     `json:"maxSize"`
	Lookback  float64 `json:"lookbackSec"`
	Dedup     bool    `json:"dedup"`
}

func parseStrategy(s string) (digestr.Strategy, error) {
	switch s {
	case "", "fixed":
		return digestr.Fixed, nil
	case "backoff":
		return digestr.Backoff, nil
	default:
		return 0, badArg("invalid_argument", fmt.Sprintf("strategy must be \"fixed\" or \"backoff\", got %q", s))
	}
}

// build validates a config spec and constructs a fresh Digester. New returns a
// typed sentinel if the config is unusable; the bridge surfaces its code.
func (spec configSpec) build() (*session, error) {
	strat, err := parseStrategy(spec.Strategy)
	if err != nil {
		return nil, err
	}
	if spec.WindowSec <= 0 || spec.WindowSec > maxWindowSeconds {
		return nil, badArg("bad_window", fmt.Sprintf("window must be between 0 (exclusive) and %d seconds", maxWindowSeconds))
	}
	if spec.MaxSize < 0 || spec.MaxSize > maxSizeCap {
		return nil, badArg("bad_size", fmt.Sprintf("maxSize must be between 0 and %d (0 disables the size trigger)", maxSizeCap))
	}
	if spec.Lookback < 0 || spec.Lookback > maxLookbackSeconds {
		return nil, badArg("bad_lookback", fmt.Sprintf("lookback must be between 0 and %d seconds", maxLookbackSeconds))
	}
	window := time.Duration(spec.WindowSec * float64(time.Second))
	lookback := time.Duration(spec.Lookback * float64(time.Second))

	cfg := digestr.Config[event]{
		Key:      recipientKey,
		Window:   window,
		Strategy: strat,
		MaxSize:  spec.MaxSize,
		Lookback: lookback,
	}
	if spec.Dedup {
		cfg.Dedup = dedupID
	}
	d, err := digestr.New(cfg)
	if err != nil {
		return nil, err
	}
	return &session{
		d:         d,
		window:    window,
		maxSize:   spec.MaxSize,
		strategy:  strat,
		lookback:  lookback,
		dedup:     spec.Dedup,
		clock:     sessionBase,
		recipSeen: map[string]bool{},
	}, nil
}

// handleNewSession builds a fresh digester from the config the page describes and
// stores it as a live session. An id the page reuses overwrites its own session,
// so a config change resets the board. It returns the whole (empty) state.
func handleNewSession(in []byte) (any, error) {
	var spec configSpec
	if err := json.Unmarshal(in, &spec); err != nil {
		return nil, err
	}
	s, err := spec.build()
	if err != nil {
		return nil, err
	}

	mu.Lock()
	id := spec.SessionID
	if id == "" {
		seq++
		id = fmt.Sprintf("s%d", seq)
	}
	if _, exists := sessions[id]; !exists && len(sessions) >= maxSessions {
		mu.Unlock()
		return nil, badArg("too_many_sessions", fmt.Sprintf("at most %d live sessions; reload the page", maxSessions))
	}
	s.id = id
	sessions[id] = s
	mu.Unlock()

	return stateOf(s, nil), nil
}

// handleReset rebuilds the session's digester with its own current config,
// clearing every window, batch and counter. Maps to constructing a fresh
// digestr.Digester with the same Config.
func handleReset(in []byte) (any, error) {
	var req struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(in, &req); err != nil {
		return nil, err
	}
	s, err := getSession(req.SessionID)
	if err != nil {
		return nil, err
	}
	spec := configSpec{
		Strategy:  strategyName(s.strategy),
		WindowSec: s.window.Seconds(),
		MaxSize:   s.maxSize,
		Lookback:  s.lookback.Seconds(),
		Dedup:     s.dedup,
	}
	fresh, err := spec.build()
	if err != nil {
		return nil, err
	}
	mu.Lock()
	fresh.id = s.id
	sessions[s.id] = fresh
	mu.Unlock()
	return stateOf(fresh, nil), nil
}

func strategyName(s digestr.Strategy) string {
	if s == digestr.Backoff {
		return "backoff"
	}
	return "fixed"
}

// --- addEvent ---------------------------------------------------------------

type addReq struct {
	SessionID string   `json:"sessionId"`
	Recipient string   `json:"recipient"`
	Kind      string   `json:"kind"`
	Body      string   `json:"body"`
	ID        string   `json:"id"`
	AtSec     *float64 `json:"atSec"`
}

// handleAddEvent offers one event to the live digester at the given instant (or
// the current clock). It maps to digestr.Digester.Add and threads the Add
// contract faithfully: a returned `immediate` is a Backoff passthrough recorded
// as a delivered lone event, and a returned `flushed` is a size-triggered batch.
// Add never time-flushes, so a window that is already past due stays open until
// a Due — the page shows it as "overdue" and a drag of the clock drains it.
//
// The clock is monotonic: an instant before the highest observed is pulled
// forward, mirroring digestr's own clamp, so firing never rewinds the board.
func handleAddEvent(in []byte) (any, error) {
	var req addReq
	if err := json.Unmarshal(in, &req); err != nil {
		return nil, err
	}
	s, err := getSession(req.SessionID)
	if err != nil {
		return nil, err
	}
	if s.eventsIn >= maxEventsPerSess {
		return stateOf(s, badArg("too_many_events", fmt.Sprintf("this session has reached its %d-event cap; reset to continue", maxEventsPerSess))), nil
	}
	if req.Recipient == "" {
		return stateOf(s, badArg("invalid_argument", "an event needs a recipient")), nil
	}
	if err := boundStr("recipient", req.Recipient); err != nil {
		return stateOf(s, err), nil
	}
	for field, v := range map[string]string{"kind": req.Kind, "body": req.Body, "id": req.ID} {
		if err := boundStr(field, v); err != nil {
			return stateOf(s, err), nil
		}
	}

	now := s.clock
	if req.AtSec != nil {
		if *req.AtSec < 0 || *req.AtSec > maxAtSeconds {
			return stateOf(s, badArg("invalid_argument", "atSec out of range")), nil
		}
		now = atTime(*req.AtSec)
	}
	if now.Before(s.clock) {
		now = s.clock // monotonic clamp, matching digestr
	}

	s.evSeq++
	s.eventsIn++
	s.seeRecipient(req.Recipient)
	ev := event{
		Recipient: req.Recipient,
		Kind:      req.Kind,
		Body:      req.Body,
		ID:        req.ID,
		Seq:       s.evSeq,
		AtSec:     secOf(now),
	}

	imm, flushed := s.d.Add(now, ev)
	if flushed != nil {
		s.recordBatch(*flushed, now)
	}
	if len(imm) > 0 {
		// A Backoff passthrough: a lone event delivered immediately, opening no
		// window. Record it as a one-event delivery so the ledger counts it.
		s.batchSeq++
		s.emitted = append(s.emitted, emitted{
			Seq:        s.batchSeq,
			Key:        ev.Recipient,
			Events:     imm,
			OpenedSec:  secOf(now),
			DueSec:     secOf(now),
			FlushedSec: secOf(now),
			Reason:     "Passthrough",
		})
	}
	s.clock = now
	return stateOf(s, nil), nil
}

func boundStr(field, v string) error {
	if len(v) > maxStrLen {
		return badArg("invalid_argument", fmt.Sprintf("%s is too long (max %d characters)", field, maxStrLen))
	}
	return nil
}

// --- advanceClock / flush ---------------------------------------------------

type clockReq struct {
	SessionID string  `json:"sessionId"`
	AtSec     float64 `json:"atSec"`
}

// handleAdvanceClock moves the virtual clock to the given instant and drains
// every window whose time trigger has fired. It maps to digestr.Digester.Due:
// Due(now) flushes and closes every window whose DueAt is at or before now,
// Reason Elapsed. The clock is monotonic — a now before the current clock is
// pulled forward — so a drag left never un-fires a batch.
func handleAdvanceClock(in []byte) (any, error) {
	var req clockReq
	if err := json.Unmarshal(in, &req); err != nil {
		return nil, err
	}
	s, err := getSession(req.SessionID)
	if err != nil {
		return nil, err
	}
	if req.AtSec < 0 || req.AtSec > maxAtSeconds {
		return stateOf(s, badArg("invalid_argument", "atSec out of range")), nil
	}
	now := atTime(req.AtSec)
	if now.Before(s.clock) {
		now = s.clock
	}
	for _, b := range s.d.Due(now) {
		s.recordBatch(b, now)
	}
	s.clock = now
	return stateOf(s, nil), nil
}

// handleFlush drains every currently-overdue window without moving the clock —
// digestr.Digester.Due at the current clock. Add never time-flushes, so a window
// can sit past due until a Due call; this button demonstrates that boundary.
func handleFlush(in []byte) (any, error) {
	var req struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(in, &req); err != nil {
		return nil, err
	}
	s, err := getSession(req.SessionID)
	if err != nil {
		return nil, err
	}
	for _, b := range s.d.Due(s.clock) {
		s.recordBatch(b, s.clock)
	}
	return stateOf(s, nil), nil
}

// --- state ------------------------------------------------------------------

func handleState(in []byte) (any, error) {
	var req struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(in, &req); err != nil {
		return nil, err
	}
	s, err := getSession(req.SessionID)
	if err != nil {
		return nil, err
	}
	return stateOf(s, nil), nil
}

// --- state rendering --------------------------------------------------------

type recipientDTO struct {
	Key         string   `json:"key"`
	Events      []event  `json:"events"`
	Size        int      `json:"size"`
	HasWindow   bool     `json:"hasWindow"`
	OpenedSec   float64  `json:"openedSec"`
	DueSec      float64  `json:"dueSec"`
	Overdue     bool     `json:"overdue"`
	LastSendSec *float64 `json:"lastSendSec,omitempty"`
	HorizonSec  *float64 `json:"horizonSec,omitempty"`
}

type batchDTO struct {
	Seq        int     `json:"seq"`
	Key        string  `json:"key"`
	Events     []event `json:"events"`
	OpenedSec  float64 `json:"openedSec"`
	DueSec     float64 `json:"dueSec"`
	FlushedSec float64 `json:"flushedSec"`
	Reason     string  `json:"reason"`
}

type stateDTO struct {
	SessionID    string         `json:"sessionId"`
	Strategy     string         `json:"strategy"`
	WindowSec    float64        `json:"windowSec"`
	MaxSize      int            `json:"maxSize"`
	LookbackSec  float64        `json:"lookbackSec"`
	Dedup        bool           `json:"dedup"`
	ClockSec     float64        `json:"clockSec"`
	EventsIn     int            `json:"eventsIn"`
	DigestsOut   int            `json:"digestsOut"`
	Passthroughs int            `json:"passthroughs"`
	Recipients   []recipientDTO `json:"recipients"`
	Batches      []batchDTO     `json:"batches"`
	Error        string         `json:"error,omitempty"`
	Code         string         `json:"code,omitempty"`
}

// stateOf reads the live digester's Snapshot and assembles the one value the page
// renders from. The pending windows and the Backoff recency markers come from
// digestr.Digester.Snapshot; the emitted batches and counters are the bridge's
// own record of what Add and Due have returned. opErr, when non-nil, is an
// expected rejection (a cap hit, a missing recipient) carried back beside the
// unchanged state so the page can show it without losing the board.
func stateOf(s *session, opErr error) any {
	snap := s.d.Snapshot()

	winByKey := make(map[string]digestr.WindowState[event], len(snap.Windows))
	for _, w := range snap.Windows {
		winByKey[w.Key] = w
	}
	lastByKey := make(map[string]time.Time, len(snap.LastSend))
	for _, kt := range snap.LastSend {
		lastByKey[kt.Key] = kt.Time
	}

	clockSec := secOf(s.clock)
	recips := make([]recipientDTO, 0, len(s.recipOrder))
	for _, key := range s.recipOrder {
		dto := recipientDTO{Key: key, Events: []event{}}
		if w, ok := winByKey[key]; ok {
			dto.HasWindow = true
			dto.Events = w.Events
			dto.Size = len(w.Events)
			dto.OpenedSec = secOf(w.OpenedAt)
			dto.DueSec = secOf(w.DueAt)
			dto.Overdue = !w.DueAt.After(s.clock)
		}
		if s.strategy == digestr.Backoff {
			if t, ok := lastByKey[key]; ok {
				ls := secOf(t)
				hz := secOf(t.Add(s.lookback))
				dto.LastSendSec = &ls
				dto.HorizonSec = &hz
			}
		}
		recips = append(recips, dto)
	}

	batches := make([]batchDTO, 0, len(s.emitted))
	digests, passthroughs := 0, 0
	for _, e := range s.emitted {
		if e.Reason == "Passthrough" {
			passthroughs++
		} else {
			digests++
		}
		evs := e.Events
		if evs == nil {
			evs = []event{}
		}
		batches = append(batches, batchDTO{
			Seq:        e.Seq,
			Key:        e.Key,
			Events:     evs,
			OpenedSec:  e.OpenedSec,
			DueSec:     e.DueSec,
			FlushedSec: e.FlushedSec,
			Reason:     e.Reason,
		})
	}

	out := stateDTO{
		SessionID:    s.id,
		Strategy:     strategyName(s.strategy),
		WindowSec:    s.window.Seconds(),
		MaxSize:      s.maxSize,
		LookbackSec:  s.lookback.Seconds(),
		Dedup:        s.dedup,
		ClockSec:     clockSec,
		EventsIn:     s.eventsIn,
		DigestsOut:   digests,
		Passthroughs: passthroughs,
		Recipients:   recips,
		Batches:      batches,
	}
	if opErr != nil {
		out.Error = opErr.Error()
		out.Code = errCode(opErr)
	}
	return out
}

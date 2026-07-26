package digestr

import "errors"

// Sentinel errors returned by [New] and [Restore] when a [Config] is
// unusable. Match them with [errors.Is] rather than by comparing error
// strings.
var (
	// ErrNoKey is returned when a Config has a nil Key function. digestr groups
	// events by the string that Key returns for each; without it there is no
	// grouping and nothing to do.
	ErrNoKey = errors.New("digestr: nil Key function")

	// ErrBadWindow is returned when a Config's Window is not strictly positive.
	// A window is the span an open digest stays open for; a zero or negative
	// span cannot hold events.
	ErrBadWindow = errors.New("digestr: Window must be positive")

	// ErrBadSize is returned when a Config's MaxSize is negative. Zero is valid
	// and means "no size trigger"; a positive value is the count ceiling that
	// flushes a window early.
	ErrBadSize = errors.New("digestr: MaxSize must not be negative")

	// ErrBadLookback is returned when a Config selects the Backoff strategy but
	// its Lookback is not strictly positive. Backoff decides passthrough versus
	// digest by whether a send happened within the last Lookback, so a zero or
	// negative horizon is meaningless.
	ErrBadLookback = errors.New("digestr: Backoff requires a positive Lookback")

	// ErrBadStrategy is returned when a Config names a Strategy that is neither
	// [Fixed] nor [Backoff].
	ErrBadStrategy = errors.New("digestr: unknown strategy")

	// ErrBadSnapshot is returned by [Restore] when a Snapshot is malformed — for
	// example, two windows sharing one key, which would silently drop events. A
	// Snapshot produced by [Digester.Snapshot] is always well-formed; this
	// guards against a corrupt or hand-built one.
	ErrBadSnapshot = errors.New("digestr: malformed snapshot")
)

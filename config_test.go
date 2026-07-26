package digestr

import (
	"errors"
	"testing"
	"time"
)

func TestConfigValidation(t *testing.T) {
	t.Run("Given assorted configurations", func(t *testing.T) {
		cases := []struct {
			name string
			cfg  Config[note]
			want error // nil means valid
		}{
			{
				name: "well-formed Fixed",
				cfg:  Config[note]{Key: key, Window: time.Minute},
			},
			{
				name: "well-formed Backoff",
				cfg:  Config[note]{Key: key, Window: time.Minute, Strategy: Backoff, Lookback: time.Hour},
			},
			{
				name: "valid with size trigger",
				cfg:  Config[note]{Key: key, Window: time.Minute, MaxSize: 5},
			},
			{
				name: "nil Key",
				cfg:  Config[note]{Window: time.Minute},
				want: ErrNoKey,
			},
			{
				name: "zero Window",
				cfg:  Config[note]{Key: key},
				want: ErrBadWindow,
			},
			{
				name: "negative Window",
				cfg:  Config[note]{Key: key, Window: -time.Second},
				want: ErrBadWindow,
			},
			{
				name: "negative MaxSize",
				cfg:  Config[note]{Key: key, Window: time.Minute, MaxSize: -1},
				want: ErrBadSize,
			},
			{
				name: "Backoff with zero Lookback",
				cfg:  Config[note]{Key: key, Window: time.Minute, Strategy: Backoff},
				want: ErrBadLookback,
			},
			{
				name: "Backoff with negative Lookback",
				cfg:  Config[note]{Key: key, Window: time.Minute, Strategy: Backoff, Lookback: -time.Second},
				want: ErrBadLookback,
			},
			{
				name: "unknown strategy",
				cfg:  Config[note]{Key: key, Window: time.Minute, Strategy: Strategy(42)},
				want: ErrBadStrategy,
			},
		}
		for _, tc := range cases {
			t.Run("When constructing with "+tc.name, func(t *testing.T) {
				t.Run("Then New reports the expected result", func(t *testing.T) {
					d, err := New(tc.cfg)
					if tc.want == nil {
						if err != nil {
							t.Fatalf("unexpected error: %v", err)
						}
						if d == nil {
							t.Fatal("New returned nil digester for a valid config")
						}
						return
					}
					if !errors.Is(err, tc.want) {
						t.Fatalf("error = %v, want %v", err, tc.want)
					}
					if d != nil {
						t.Fatalf("digester = %v, want nil on error", d)
					}
				})
				t.Run("Then Restore rejects the same invalid config", func(t *testing.T) {
					if tc.want == nil {
						return
					}
					_, err := Restore(tc.cfg, Snapshot[note]{})
					if !errors.Is(err, tc.want) {
						t.Fatalf("Restore error = %v, want %v", err, tc.want)
					}
				})
			})
		}
	})
}

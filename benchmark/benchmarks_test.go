package benchmark

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
)

var errTestBoom = errors.New("boom")

func TestRunConcurrentRunsEveryTask(t *testing.T) {
	tests := map[string]struct {
		n  int
		co int
	}{
		"single worker":          {n: 100, co: 1},
		"many workers":           {n: 1000, co: 16},
		"more workers than work": {n: 3, co: 8},
		"no work":                {n: 0, co: 4},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var calls int64

			workers := sync.Map{}

			err := runConcurrent(context.Background(), tt.n, tt.co, func(_ context.Context, worker int) error {
				atomic.AddInt64(&calls, 1)
				workers.Store(worker, struct{}{})

				return nil
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if calls != int64(tt.n) {
				t.Fatalf("expected %d calls, got %d", tt.n, calls)
			}

			workers.Range(func(k, _ any) bool {
				if w, ok := k.(int); !ok || w < 0 || w >= tt.co {
					t.Errorf("worker index %v out of range [0, %d)", k, tt.co)
				}

				return true
			})
		})
	}
}

func TestRunConcurrentReturnsFirstErrorAndCancels(t *testing.T) {
	const n = 1_000_000

	var calls int64

	err := runConcurrent(context.Background(), n, 16, func(ctx context.Context, _ int) error {
		if atomic.AddInt64(&calls, 1) == 50 {
			return errTestBoom
		}

		// Once cancelled, other workers report the context error; it must
		// not replace the first error.
		return ctx.Err()
	})

	if !errors.Is(err, errTestBoom) {
		t.Fatalf("expected %v, got %v", errTestBoom, err)
	}

	if calls >= n {
		t.Fatalf("remaining work was not cancelled: %d of %d calls ran", calls, n)
	}
}

func TestRunConcurrentRespectsParentContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var calls int64

	err := runConcurrent(ctx, 100, 4, func(context.Context, int) error {
		atomic.AddInt64(&calls, 1)

		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected %v, got %v", context.Canceled, err)
	}

	if calls != 0 {
		t.Fatalf("expected no calls with a cancelled parent context, got %d", calls)
	}
}

func TestRunConcurrentParentCancelledMidRun(t *testing.T) {
	const n = 1_000_000

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls int64

	err := runConcurrent(ctx, n, 8, func(context.Context, int) error {
		if atomic.AddInt64(&calls, 1) == 50 {
			cancel()
		}

		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected %v, got %v", context.Canceled, err)
	}

	if calls >= n {
		t.Fatalf("remaining work was not cancelled: %d of %d calls ran", calls, n)
	}
}

func TestRunConcurrentParentCancelledAfterAllWork(t *testing.T) {
	const n = 100

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls int64

	err := runConcurrent(ctx, n, 4, func(context.Context, int) error {
		if atomic.AddInt64(&calls, 1) == n {
			// the final task cancels; every task still ran, so this is a success
			cancel()
		}

		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if calls != n {
		t.Fatalf("expected %d calls, got %d", n, calls)
	}
}

func TestPerQuery(t *testing.T) {
	tests := map[string]struct {
		total    uint64
		queries  int
		expected int
	}{
		"even division":    {total: 100, queries: 10, expected: 10},
		"truncates":        {total: 10, queries: 3, expected: 3},
		"zero queries":     {total: 100, queries: 0, expected: 0},
		"negative queries": {total: 100, queries: -1, expected: 0},
		"clamps to MaxInt": {total: math.MaxUint64, queries: 1, expected: math.MaxInt},
		"zero total":       {total: 0, queries: 5, expected: 0},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if actual := perQuery(tt.total, tt.queries); actual != tt.expected {
				t.Fatalf("perQuery(%d, %d): expected %d, got %d", tt.total, tt.queries, tt.expected, actual)
			}
		})
	}
}

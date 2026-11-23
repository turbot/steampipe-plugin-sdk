package plugin

import (
	"context"
	"errors"
	"testing"
)

// retryNamedHydrate converts retryConfig.MaxAttempts (int64) to a uint64
// retry count. It used to guard that conversion with `!= 0` instead of
// `> 0`, so a negative MaxAttempts wrapped to a near-infinite uint64
// instead of falling back to the documented default of 10.
//
// This drives retryNamedHydrate directly with MaxAttempts: -1 and a
// hydrate function that always reports a retryable error, capping the
// hydrate function's own failure count well above 10 so the test cannot
// hang even if the guard regresses - it just proves the retry count stays
// near the default instead of running away.
func TestRetryNamedHydrateNegativeMaxAttemptsUsesDefault(t *testing.T) {
	const bailOutAfter = 30 // far above the documented default of 10

	d := &QueryData{Table: &Table{Name: "t", Plugin: &Plugin{Name: "p"}}}
	hydrateData := &HydrateData{}

	attempts := 0
	hydrate := newNamedHydrateFunc(func(ctx context.Context, d *QueryData, h *HydrateData) (interface{}, error) {
		attempts++
		if attempts >= bailOutAfter {
			// stop the loop regardless of the guard, so this test cannot hang
			return "ok", nil
		}
		return nil, errors.New("transient error")
	})

	retryConfig := &RetryConfig{
		MaxAttempts:      -1,
		BackoffAlgorithm: "Constant",
		RetryInterval:    1, // ms - keep this test fast
		ShouldRetryErrorFunc: func(context.Context, *QueryData, *HydrateData, error) bool {
			return true
		},
	}

	_, _ = retryNamedHydrate(context.Background(), d, hydrateData, hydrate, retryConfig)

	// go-retry's WithMaxRetries allows the initial attempt plus up to
	// MaxAttempts retries, so the default of 10 means exactly 11 calls.
	const expectedAttempts = 11
	if attempts != expectedAttempts {
		t.Fatalf("expected retryNamedHydrate to fall back to the default of 10 retries for a negative MaxAttempts (%d calls), got %d attempts", expectedAttempts, attempts)
	}
}

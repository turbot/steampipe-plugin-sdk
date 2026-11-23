package plugin_test

// Tests for a plugin that does not set Plugin.ClientWaitsForHydrateRateLimit: each hydrate call waits
// for its rate limiters in its own goroutine, so a row reserves the tokens of all its calls at once.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v6/rate_limiter"
)

func rlRowValue(_ context.Context, _ *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	return h.Item.(rlWidget).ID, nil
}

// five hydrate functions, each with its own name (the SDK names a hydrate call after its Go function)
func rlCall1(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	return rlRowValue(ctx, d, h)
}
func rlCall2(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	return rlRowValue(ctx, d, h)
}
func rlCall3(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	return rlRowValue(ctx, d, h)
}
func rlCall4(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	return rlRowValue(ctx, d, h)
}
func rlCall5(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	return rlRowValue(ctx, d, h)
}

var rlUnlimitedStarted atomic.Int64

// rlUnlimitedCall has no rate limiter tags, so no limiter applies to it
func rlUnlimitedCall(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (any, error) {
	rlUnlimitedStarted.Add(1)
	return rlRowValue(ctx, d, h)
}

func TestSDKWaits_RowArrivesAsSoonAsItsOwnTokensDo(t *testing.T) {
	// 40 rows, each with 5 rate limited calls, under one limiter of 200 tokens a second:
	// the query needs 200 tokens, so it takes about a second.
	// A row that waited for its calls' tokens one after another would arrive only when most of the
	// query's tokens were gone; a row that reserves all five at once arrives within a few tens of ms.
	f := rlFixture{
		rows:    40,
		limiter: &rate_limiter.Definition{Name: rlLimiter, FillRate: 200, BucketSize: 1, Scope: []string{"connection"}, Where: "service = 'widget'"},
		columns: map[string]plugin.HydrateConfig{
			"a": {Func: rlCall1, Tags: widgetTags},
			"b": {Func: rlCall2, Tags: widgetTags},
			"c": {Func: rlCall3, Tags: widgetTags},
			"d": {Func: rlCall4, Tags: widgetTags},
			"e": {Func: rlCall5, Tags: widgetTags},
		},
	}
	rows, arrivals, finished := newRlServer(t, f).queryWithArrivals(t, []string{"id", "a", "b", "c", "d", "e"}, rlFinishTimeout)
	if !finished || len(rows) != 40 {
		t.Fatalf("finished %v, %d rows, want 40", finished, len(rows))
	}
	if arrivals[0] > 400*time.Millisecond {
		t.Fatalf("the first row arrived after %v, want under 400 ms: its calls did not reserve their tokens at once", arrivals[0])
	}
	// the number of tokens does not change, so neither does the total time (about 1 s)
	last := arrivals[len(arrivals)-1]
	if last < 800*time.Millisecond {
		t.Fatalf("all rows arrived after %v, want about 1 s: the query used fewer tokens than it needs", last)
	}
}

func TestSDKWaits_CallWithNoLimiterDoesNotWaitForTheRowsLimitedCalls(t *testing.T) {
	rlUnlimitedStarted.Store(0)
	// one token: the limited calls of every row but one wait for the whole query, and the query is cancelled
	// after a second. The row's call with no limiter must have run in every row anyway.
	f := rlFixture{
		rows:    5,
		limiter: slowLimiter(1),
		columns: map[string]plugin.HydrateConfig{
			"a":    {Func: rlCall1, Tags: widgetTags},
			"b":    {Func: rlCall2, Tags: widgetTags},
			"free": {Func: rlUnlimitedCall},
		},
	}
	if _, finished := newRlServer(t, f).query(t, []string{"id", "a", "b", "free"}, rlWaitTimeout); finished {
		t.Fatalf("the query finished with 1 token, but its rows need 10")
	}
	if got := rlUnlimitedStarted.Load(); got != 5 {
		t.Fatalf("the call with no limiter started in %d of 5 rows: it waited for its row's limited calls", got)
	}
}

func TestSDKWaits_EachCallUsesItsOwnToken(t *testing.T) {
	// 3 rows with 2 limited calls each: 6 tokens
	twoCalls := func(budget int64) rlFixture {
		return rlFixture{
			rows:    3,
			limiter: slowLimiter(budget),
			columns: map[string]plugin.HydrateConfig{
				"a": {Func: rlCall1, Tags: widgetTags},
				"b": {Func: rlCall2, Tags: widgetTags},
			},
		}
	}
	assertTokens(t, twoCalls, []string{"id", "a", "b"}, 6)
}

func TestSDKWaits_KeepsMaxConcurrency(t *testing.T) {
	rlInFlight.Store(0)
	rlMaxInFlight.Store(0)
	f := rlFixture{
		rows: 5,
		limiter: &rate_limiter.Definition{
			Name:           rlLimiter,
			MaxConcurrency: 1,
			Scope:          []string{"connection"},
			Where:          "service = 'widget'",
		},
		columns: map[string]plugin.HydrateConfig{"value": {Func: sendSlowRequest, Tags: widgetTags}},
	}
	rows, finished := newRlServer(t, f).query(t, []string{"id", "value"}, rlFinishTimeout)
	if !finished || len(rows) != 5 {
		t.Fatalf("finished %v, %d rows, want 5", finished, len(rows))
	}
	if got := rlMaxInFlight.Load(); got != 1 {
		t.Fatalf("%d calls ran at once, want 1", got)
	}
}

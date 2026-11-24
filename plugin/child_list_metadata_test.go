package plugin

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/rate_limiter"
	"golang.org/x/time/rate"
)

// runParentChildList runs a parent-child list of three parents, each with one child row, and returns the rate
// limiter delay each row's diagnostics report for its child list call, by parent.
//
// The limiter holds one token and refills one every 20ms. The parent waits 40ms before its third item, so the
// third child list call finds a token waiting (no delay) and the first two wait for one.
// With holdChildren, the child list calls stay blocked until the parent list has started all of them, so each
// one reads its metadata after every later child list call has been started. Without it, each keeps reading its
// metadata until then, while the parent list starts the next child list call.
func runParentChildList(t *testing.T, holdChildren bool) map[int]int64 {
	t.Helper()
	t.Setenv(EnvDiagnosticsLevel, DiagnosticsAll)

	def := &rate_limiter.Definition{Name: "child_limiter", FillRate: rate.Limit(50), BucketSize: 1}
	if err := def.Initialise(); err != nil {
		t.Fatalf("limiter definition: %v", err)
	}
	p := &Plugin{Name: "p", rateLimiterInstances: rate_limiter.NewLimiterMap()}
	p.resolvedRateLimiterDefs = map[string]*rate_limiter.Definition{def.Name: def}

	const parents = 3
	release := make(chan struct{})
	table := &Table{Name: "t", Plugin: p}
	table.List = &ListConfig{
		ParentHydrate: func(ctx context.Context, d *QueryData, _ *HydrateData) (interface{}, error) {
			for i := 0; i < parents; i++ {
				if i == parents-1 {
					time.Sleep(40 * time.Millisecond)
				}
				d.StreamListItem(ctx, i)
			}
			return nil, nil
		},
		Hydrate: func(ctx context.Context, d *QueryData, h *HydrateData) (interface{}, error) {
			if holdChildren {
				<-release
			} else {
				// read the metadata, as building a row's _ctx does, while the parent list starts the next child
				rd := newRowData(d, h.Item)
				for open := true; open; {
					if _, err := json.Marshal(newRowCtxData(rd)); err != nil {
						t.Errorf("marshal _ctx: %v", err)
					}
					select {
					case <-release:
						open = false
					default:
					}
				}
			}
			d.StreamListItem(ctx, h.Item)
			return nil, nil
		},
	}
	table.List.initialise(table)

	d := newListRateLimitTestQueryData(p, table)
	d.rowDataChan = make(chan *rowData, parents)
	d.setListCalls(table.List.namedParentHydrate, table.List.namedHydrate)
	d.listWg = &sync.WaitGroup{}

	table.doList(context.Background(), d, table.List.namedParentHydrate)
	close(release)
	d.listWg.Wait()
	close(d.rowDataChan)

	delays := map[int]int64{}
	for rd := range d.rowDataChan {
		calls := newRowCtxData(rd).Diagnostics.Calls
		// the parent list call, then the child list call
		if len(calls) < 2 {
			t.Fatalf("expected the parent and child list calls in the row's diagnostics, got %d call(s)", len(calls))
		}
		if calls[1].FuncName != table.List.namedHydrate.Name {
			t.Fatalf("expected the second call to be the child list %q, got %q", table.List.namedHydrate.Name, calls[1].FuncName)
		}
		delays[rd.item.(int)] = calls[1].DelayMs
	}
	if len(delays) != parents {
		t.Fatalf("expected a row from each of %d child list calls, got %d", parents, len(delays))
	}
	return delays
}

// Each row's diagnostics show the rate limiter delay of the child list call that returned it,
// however late the row reads it (the issue: they showed the delay of whichever child list call wrote last).
func TestChildListRowsShowTheirOwnRateLimitDelay(t *testing.T) {
	delays := runParentChildList(t, true)
	for _, parent := range []int{0, 1} {
		if delays[parent] <= 0 {
			t.Errorf("child list call %d waited for a token, but its row shows a delay of %dms", parent, delays[parent])
		}
	}
	if delays[2] != 0 {
		t.Errorf("child list call 2 found a token waiting, but its row shows a delay of %dms", delays[2])
	}
}

// Rows read the list metadata while the next child list call is started: run with -race (the issue's data race)
func TestChildListMetadataDoesNotRace(t *testing.T) {
	for i := 0; i < 20; i++ {
		runParentChildList(t, false)
	}
}

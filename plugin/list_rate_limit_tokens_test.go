package plugin

import (
	"context"
	"sync"
	"testing"

	"github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v6/rate_limiter"
	"golang.org/x/time/rate"
)

// The limiters in these tests refill so slowly that no token comes back during a test, and a test that
// takes more tokens than the bucket holds is told so by a delay instead of waiting (see tokensLeft).
const listTestBucketSize = 10

func newListRateLimitTestPlugin(t *testing.T) *Plugin {
	t.Helper()
	def := &rate_limiter.Definition{Name: "list_limiter", FillRate: rate.Limit(0.0001), BucketSize: listTestBucketSize}
	if err := def.Initialise(); err != nil {
		t.Fatalf("limiter definition: %v", err)
	}
	p := &Plugin{Name: "p", rateLimiterInstances: rate_limiter.NewLimiterMap()}
	p.resolvedRateLimiterDefs = map[string]*rate_limiter.Definition{def.Name: def}
	return p
}

func newListRateLimitTestQueryData(p *Plugin, table *Table) *QueryData {
	d := &QueryData{
		Table:            table,
		plugin:           p,
		Connection:       &Connection{Name: "c"},
		connectionCallId: "test",
		FetchType:        fetchTypeList,
		EqualsQuals:      map[string]*proto.QualValue{},
		Quals:            KeyColumnQualMap{},
		errorChan:        make(chan error, 10),
		rowDataChan:      make(chan *rowData, 10),
		listWg:           &sync.WaitGroup{},
		queryStatus:      newQueryStatus(nil),
	}
	d.StreamListItem = d.streamListItem
	return d
}

// tokensLeft counts the tokens the list call's limiter still has, by taking them with an already cancelled
// context: a Wait with no token left returns a delay at once, and the cancelled context hands its reservation back.
func tokensLeft(t *testing.T, p *Plugin, d *QueryData) int {
	t.Helper()
	limiter, err := p.getHydrateCallRateLimiter(d.Table.List.Tags, d)
	if err != nil {
		t.Fatalf("getHydrateCallRateLimiter: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	left := 0
	for left < listTestBucketSize+1 && limiter.Wait(ctx) == 0 {
		left++
	}
	return left
}

// how a test list function waits for the list rate limit as it fetches its pages
type pageWait int

const (
	// waits before each page, the first one included, as design/rate_limiters.md shows
	waitBeforeEachPage pageWait = iota
	// gets its first page with no wait and waits only before each later page, as the Azure plugin's
	// `for result.NotDone() { d.WaitForListRateLimit(ctx); result.NextWithContext(ctx) }` loops do
	waitBeforeLaterPages
	// never waits
	neverWait
)

// listPages is a list function body that fetches `pages` pages, waiting for the list rate limit as `wait` says,
// and streams one item from each page
func listPages(ctx context.Context, d *QueryData, pages int, wait pageWait) {
	for i := 0; i < pages; i++ {
		if wait == waitBeforeEachPage || (wait == waitBeforeLaterPages && i > 0) {
			d.WaitForListRateLimit(ctx)
		}
		d.StreamListItem(ctx, i+1)
	}
}

// newListTable builds a table whose list call fetches `pages` pages, waiting for the list rate limit as `wait` says
func newListTable(pages int, wait pageWait) *Table {
	table := &Table{Name: "t"}
	table.List = &ListConfig{
		Hydrate: func(ctx context.Context, d *QueryData, _ *HydrateData) (interface{}, error) {
			listPages(ctx, d, pages, wait)
			return nil, nil
		},
	}
	table.List.initialise(table)
	return table
}

func TestListCallTokens(t *testing.T) {
	tests := []struct {
		name     string
		pages    int
		wait     pageWait
		wantUsed int
	}{
		// the issue: a one page list that waits before its page used two tokens
		{"one page, waits before the page", 1, waitBeforeEachPage, 1},
		{"three pages, waits before each", 3, waitBeforeEachPage, 3},
		// a list function which never waits still uses the one token the SDK takes before calling it
		{"one page, never waits", 1, neverWait, 1},
		{"three pages, never waits", 3, neverWait, 1},
		// a list function whose first page comes with no wait: the SDK's token pays for that page, and each
		// later page waits for its own token (the first wait comes after an item has streamed, so it is not free)
		{"one page, waits only before later pages", 1, waitBeforeLaterPages, 1},
		{"three pages, waits only before later pages", 3, waitBeforeLaterPages, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := newListRateLimitTestPlugin(t)
			table := newListTable(tc.pages, tc.wait)
			table.Plugin = p
			d := newListRateLimitTestQueryData(p, table)
			d.listHydrate = table.List.namedHydrate

			table.doList(context.Background(), d, table.List.namedHydrate)

			if got := listTestBucketSize - tokensLeft(t, p, d); got != tc.wantUsed {
				t.Fatalf("expected the list call to use %d token(s), it used %d", tc.wantUsed, got)
			}
		})
	}
}

// each matrix item (a region, for example) is its own list call and uses one token for a one page list
func TestListCallTokensWithMatrix(t *testing.T) {
	p := newListRateLimitTestPlugin(t)
	table := newListTable(1, waitBeforeEachPage)
	table.Plugin = p
	d := newListRateLimitTestQueryData(p, table)
	d.listHydrate = table.List.namedHydrate
	items := []map[string]any{{"region": "a"}, {"region": "b"}, {"region": "c"}}
	d.Matrix = items
	d.filteredMatrix = items

	table.doList(context.Background(), d, table.List.namedHydrate)

	if got := listTestBucketSize - tokensLeft(t, p, d); got != len(items) {
		t.Fatalf("expected %d tokens for %d matrix items of one page each, used %d", len(items), len(items), got)
	}
}

// A parent-child list: the SDK waits once for the parent list call and once before each child list call.
// Each of those waits pays for the first page of the call it comes before, whether or not the parent
// list function waits, so a child that waits before its first page uses no token beyond the SDK's,
// and a child that waits only before its later pages pays one token for each of them.
func TestParentChildListCallTokens(t *testing.T) {
	const children = 3
	tests := []struct {
		name        string
		parentWait  pageWait
		childPages  int
		childWait   pageWait
		perChildUse int
	}{
		{"parent waits before its page", waitBeforeEachPage, 1, waitBeforeEachPage, 1},
		{"parent never waits", neverWait, 1, waitBeforeEachPage, 1},
		{"children wait before each of two pages", neverWait, 2, waitBeforeEachPage, 2},
		{"children wait only before their second page", waitBeforeEachPage, 2, waitBeforeLaterPages, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := newListRateLimitTestPlugin(t)
			table := &Table{Name: "t", Plugin: p}
			table.List = &ListConfig{
				// one page holding one item per child list call
				ParentHydrate: func(ctx context.Context, d *QueryData, _ *HydrateData) (interface{}, error) {
					if tc.parentWait == waitBeforeEachPage {
						d.WaitForListRateLimit(ctx)
					}
					for i := 0; i < children; i++ {
						d.StreamListItem(ctx, i)
					}
					return nil, nil
				},
				Hydrate: func(ctx context.Context, d *QueryData, _ *HydrateData) (interface{}, error) {
					listPages(ctx, d, tc.childPages, tc.childWait)
					return nil, nil
				},
			}
			table.List.initialise(table)

			d := newListRateLimitTestQueryData(p, table)
			d.setListCalls(table.List.namedParentHydrate, table.List.namedHydrate)

			table.doList(context.Background(), d, table.List.namedParentHydrate)
			d.listWg.Wait()

			// one token for the parent list call and the child's pages for each child list call
			want := 1 + children*tc.perChildUse
			if got := listTestBucketSize - tokensLeft(t, p, d); got != want {
				t.Fatalf("expected %d tokens (the parent call and %d child calls of %d each), used %d", want, children, tc.perChildUse, got)
			}
		})
	}
}

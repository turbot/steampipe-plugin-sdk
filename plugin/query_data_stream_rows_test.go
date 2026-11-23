package plugin

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v6/query_cache"
)

func newStreamRowsTestQueryData() *QueryData {
	p := &Plugin{Name: "p", queryCache: &query_cache.QueryCache{}}
	return &QueryData{
		Table:        &Table{Name: "t", Plugin: p},
		plugin:       p,
		errorChan:    make(chan error, 1),
		cacheEnabled: false,
	}
}

// A failed list call puts its error on errorChan and the row data then ends, which closes rowChan.
// With both ready, streamRows used to pick the closed rowChan half the time and return nil: no error, no rows.
// The channels are filled before streamRows starts, so both cases are always ready when it reaches its select.
func TestStreamRowsReturnsErrorWhenRowChanClosedToo(t *testing.T) {
	listErr := errors.New("list call failed: 403")
	for i := 0; i < 100; i++ {
		d := newStreamRowsTestQueryData()
		rowChan := make(chan *proto.Row)
		doneChan := make(chan bool)

		d.errorChan <- listErr
		close(rowChan)

		err := d.streamRows(context.Background(), rowChan, doneChan)
		if !errors.Is(err, listErr) {
			t.Fatalf("run %d: expected streamRows to return %v, got %v", i, listErr, err)
		}
		// the error path closes doneChan to stop buildRowsAsync
		select {
		case <-doneChan:
		default:
			t.Fatalf("run %d: expected doneChan to be closed after the error", i)
		}
	}
}

// With no error, a closed rowChan is a clean end: no error, and doneChan is left open.
func TestStreamRowsReturnsNilWhenRowChanClosedWithoutError(t *testing.T) {
	for i := 0; i < 100; i++ {
		d := newStreamRowsTestQueryData()
		rowChan := make(chan *proto.Row)
		doneChan := make(chan bool)
		close(rowChan)

		if err := d.streamRows(context.Background(), rowChan, doneChan); err != nil {
			t.Fatalf("run %d: expected nil, got %v", i, err)
		}
		select {
		case <-doneChan:
			t.Fatalf("run %d: doneChan should stay open on a clean end", i)
		default:
		}
	}
}

// A child list call that panics reports the panic as an error. fetchComplete stops waiting for child list calls
// when they are done, and the row channels close after it, so the error must reach errorChan before the call
// counts as done: otherwise streamRows can see a closed row channel and an empty errorChan, and end with no error.
func TestChildListPanicErrorIsSentBeforeFetchCompletes(t *testing.T) {
	for i := 0; i < 500; i++ {
		var wg sync.WaitGroup
		p := &Plugin{Name: "p", queryCache: &query_cache.QueryCache{}}
		table := &Table{Name: "t", Plugin: p}
		table.List = &ListConfig{
			Hydrate: func(context.Context, *QueryData, *HydrateData) (interface{}, error) {
				panic("child list exploded")
			},
		}
		d := &QueryData{
			Table:         table,
			plugin:        p,
			Connection:    &Connection{Name: "c"},
			errorChan:     make(chan error, 1),
			rowDataChan:   make(chan *rowData, 1),
			listWg:        &wg,
			fetchMetadata: &hydrateMetadata{},
			queryStatus:   &queryStatus{},
			fetchLimiters: &fetchCallRateLimiters{},
		}

		d.callChildListHydrate(context.Background(), "parent item")
		// fetchComplete waits for the child list call, then closes rowDataChan
		d.fetchComplete(context.Background())

		select {
		case err := <-d.errorChan:
			if err == nil {
				t.Fatalf("run %d: got a nil error", i)
			}
		default:
			t.Fatalf("run %d: fetchComplete returned before the child list call's panic reached errorChan", i)
		}
	}
}

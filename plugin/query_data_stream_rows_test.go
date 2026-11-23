package plugin

import (
	"context"
	"errors"
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

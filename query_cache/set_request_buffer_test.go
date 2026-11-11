package query_cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/grpc"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
)

func bufferTestRow(i int) *sdkproto.Row {
	return &sdkproto.Row{Columns: map[string]*sdkproto.Column{
		"id": {Value: &sdkproto.Column_StringValue{StringValue: fmt.Sprintf("row-%d", i)}},
	}}
}

// Rows a subscriber gets from the page buffer must not change when the scan
// later fills the buffer, writes it to the cache and starts the next page.
// The subscriber streams those rows after it releases the request lock.
func TestSetRequest_BufferedRowsAreNotOverwrittenByNextPage(t *testing.T) {
	ctx := context.Background()
	qc, err := NewQueryCache("test", map[string]*grpc.PluginSchema{}, &QueryCacheOptions{Enabled: true, Ttl: 5 * time.Minute, MaxSizeMb: 256})
	if err != nil {
		t.Fatal(err)
	}
	req := newSetRequest(&CacheRequest{Table: "t", CallId: "call", ConnectionName: "conn", resultKeyRoot: "root"}, qc)
	req.subscribe(&setRequestSubscriber{callId: "subscriber"})
	qc.setRequests["call"] = req

	add := func(from, to int) {
		for i := from; i < to; i++ {
			if err := qc.IterateSet(ctx, bufferTestRow(i), "call"); err != nil {
				t.Fatal(err)
			}
		}
	}

	// the scan has produced 500 rows; the subscriber takes them from the buffer
	add(0, 500)
	got, err := req.getRowsSince(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 500 {
		t.Fatalf("got %d rows, want 500", len(got))
	}

	// before the subscriber streams them, the scan completes the page and starts the next one
	add(500, 1010)

	for i, r := range got {
		if want, have := fmt.Sprintf("row-%d", i), r.Columns["id"].GetStringValue(); have != want {
			t.Fatalf("row %d changed to %s after the next page started", i, have)
		}
	}
}

package query_cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/grpc"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
)

func testRow(i int) *sdkproto.Row {
	return &sdkproto.Row{Columns: map[string]*sdkproto.Column{
		"id": {Value: &sdkproto.Column_StringValue{StringValue: fmt.Sprintf("row-%d", i)}},
	}}
}

// newTestSetRequest builds a set request which has written `pages` full pages to the cache
// and holds `buffered` more rows in its page buffer.
func newTestSetRequest(t *testing.T, pages, buffered int) *setRequest {
	t.Helper()
	qc, err := NewQueryCache("test", map[string]*grpc.PluginSchema{}, &QueryCacheOptions{Enabled: true, Ttl: 5 * time.Minute, MaxSizeMb: 256})
	if err != nil {
		t.Fatal(err)
	}
	req := newSetRequest(&CacheRequest{Table: "t", CallId: "call", ConnectionName: "conn", resultKeyRoot: "root"}, qc)
	ctx := context.Background()
	n := 0
	for p := 0; p < pages; p++ {
		rows := make([]*sdkproto.Row, rowBufferSize)
		for i := range rows {
			rows[i] = testRow(n)
			n++
		}
		if err := doSet(ctx, getPageKey("root", p), &sdkproto.QueryResult{Rows: rows}, time.Minute, qc.cache, nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < buffered; i++ {
		req.pageBuffer[i] = testRow(n)
		n++
	}
	req.pageCount = int64(pages)
	req.bufferIndex = buffered
	req.rowCount = n
	return req
}

// A subscriber that is several pages behind gets one page per call, not its whole backlog.
func TestSetRequest_GetRowsSince_OnePagePerCall(t *testing.T) {
	const pages, buffered = 5, 300
	req := newTestSetRequest(t, pages, buffered)
	ctx := context.Background()

	streamed := 0
	calls := 0
	for {
		rows, err := req.getRowsSince(ctx, streamed)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		if len(rows) > rowBufferSize {
			t.Fatalf("call %d returned %d rows, want at most one page (%d)", calls, len(rows), rowBufferSize)
		}
		for i, r := range rows {
			want := fmt.Sprintf("row-%d", streamed+i)
			if got := r.Columns["id"].GetStringValue(); got != want {
				t.Fatalf("row %d: got %s, want %s", streamed+i, got, want)
			}
		}
		streamed += len(rows)
		calls++
		if calls > pages+2 {
			t.Fatal("too many calls")
		}
		if streamed == req.rowCount {
			break
		}
	}
	if streamed != pages*rowBufferSize+buffered {
		t.Fatalf("streamed %d rows, want %d", streamed, pages*rowBufferSize+buffered)
	}
}

// Starting part-way through a page returns the rest of that page.
func TestSetRequest_GetRowsSince_StartOffset(t *testing.T) {
	req := newTestSetRequest(t, 2, 10)
	rows, err := req.getRowsSince(context.Background(), 1500)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 500 || rows[0].Columns["id"].GetStringValue() != "row-1500" {
		t.Fatalf("got %d rows starting at %s, want 500 starting at row-1500", len(rows), rows[0].Columns["id"].GetStringValue())
	}
	// caught up with the cache: the buffered rows come next
	rows, err = req.getRowsSince(context.Background(), 2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 10 || rows[0].Columns["id"].GetStringValue() != "row-2000" {
		t.Fatalf("got %d buffered rows, want 10", len(rows))
	}
}

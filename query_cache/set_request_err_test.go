package query_cache

import (
	"context"
	"testing"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/grpc"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
)

// A scan writes its set request's error after each page it caches, while readers of the
// scan read that error under the request lock. Run with -race: the write must take the lock too.
func TestQueryCache_IterateSetAndReaderShareTheErrorSafely(t *testing.T) {
	ctx := context.Background()
	c, err := NewQueryCache("test-plugin", map[string]*grpc.PluginSchema{}, &QueryCacheOptions{Enabled: true, Ttl: time.Hour, MaxSizeMb: 64})
	if err != nil {
		t.Fatalf("NewQueryCache: %v", err)
	}

	// set up the scan as startSet does, with one reader subscribed,
	// but poll the reader here instead of starting its streaming goroutine
	req := &CacheRequest{CallId: "scan", Table: "t", Columns: []string{"id"}, Limit: -1, ConnectionName: "conn", TtlSeconds: 300}
	req.resultKeyRoot = c.buildResultKey(req)
	set := newSetRequest(req, c)
	reader := newSetRequestSubscriber(func(*sdkproto.Row) {}, "reader", ctx, set)
	set.requestLock.Lock()
	set.subscribe(reader)
	set.requestLock.Unlock()
	c.setRequestMapLock.Lock()
	c.setRequests[req.CallId] = set
	c.setRequestMapLock.Unlock()

	// the reader moves on through the rows as readAndStreamAsync does.
	// "no rows available" (the reader has caught up) is expected, so errors are not checked.
	stop := make(chan struct{})
	polled := make(chan struct{})
	go func() {
		defer close(polled)
		for {
			select {
			case <-stop:
				return
			default:
				rows, _ := reader.getRowsToStream()
				reader.rowsStreamed += len(rows)
			}
		}
	}()

	row := &sdkproto.Row{Columns: map[string]*sdkproto.Column{"id": {Value: &sdkproto.Column_StringValue{StringValue: "x"}}}}
	for i := 0; i < 20*rowBufferSize; i++ {
		if err := c.IterateSet(ctx, row, req.CallId); err != nil {
			t.Fatalf("row %d: IterateSet: %v", i, err)
		}
	}
	close(stop)
	<-polled
}

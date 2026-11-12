package query_cache

import (
	"context"
	"testing"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/grpc"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
)

// A reader whose cached page cannot be read (evicted or expired) must release the set
// request's read lock. If it does not, the scan blocks for good on its next row, because
// adding a row needs the write lock.
func TestSetRequestSubscriber_GetRowsToStream_ReleasesLockOnError(t *testing.T) {
	qc, err := NewQueryCache("test", map[string]*grpc.PluginSchema{}, &QueryCacheOptions{Enabled: true, Ttl: 5 * time.Minute, MaxSizeMb: 256})
	if err != nil {
		t.Fatal(err)
	}
	req := newSetRequest(&CacheRequest{Table: "t", CallId: "call", ConnectionName: "conn", resultKeyRoot: "root"}, qc)
	// the scan has written one page, but the page is no longer in the cache
	req.pageCount = 1
	req.rowCount = rowBufferSize

	s := newSetRequestSubscriber(func(*sdkproto.Row) {}, "reader", context.Background(), req)
	if _, err := s.getRowsToStream(); err == nil {
		t.Fatal("getRowsToStream returned no error for a page that is not in the cache")
	}

	if !req.requestLock.TryLock() {
		t.Fatal("getRowsToStream returned an error but left the set request's read lock held")
	}
	req.requestLock.Unlock()
}

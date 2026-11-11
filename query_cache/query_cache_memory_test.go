package query_cache

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/grpc"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
)

const mb = 1024 * 1024

func liveHeap() int64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc) //nolint:gosec // G115: a test heap size is far below the int64 limit
}

func newTestQueryCache(t *testing.T, maxSizeMb int, ttl time.Duration) *QueryCache {
	t.Helper()
	c, err := NewQueryCache("test-plugin", map[string]*grpc.PluginSchema{}, &QueryCacheOptions{
		Enabled:   true,
		Ttl:       ttl,
		MaxSizeMb: maxSizeMb,
	})
	if err != nil {
		t.Fatalf("NewQueryCache: %v", err)
	}
	return c
}

// writeTestResult caches the result of one scan with the query cache's page writer
// (writePageToCache): a page for every rowBufferSize rows, then a final page.
// It fills the page buffer directly, so no streaming reader runs alongside.
// Each row holds rowBytes of data, so a full page holds about rowBufferSize*rowBytes.
func writeTestResult(t *testing.T, c *QueryCache, table string, rows, rowBytes int) *CacheRequest {
	t.Helper()
	ctx := context.Background()
	req := &CacheRequest{
		CallId:         table,
		Table:          table,
		Columns:        []string{"data"},
		Limit:          -1,
		ConnectionName: "conn",
		TtlSeconds:     3600,
	}
	req.resultKeyRoot = c.buildResultKey(req)
	set := newSetRequest(req, c)

	row := &sdkproto.Row{Columns: map[string]*sdkproto.Column{
		"data": {Value: &sdkproto.Column_StringValue{StringValue: strings.Repeat("x", rowBytes)}},
	}}
	for i := 0; i < rows; i++ {
		set.pageBuffer[set.bufferIndex] = row
		set.bufferIndex++
		set.rowCount++
		if set.bufferIndex == rowBufferSize {
			if err := c.writePageToCache(ctx, set, false); err != nil {
				t.Fatalf("%s: page %d: %v", table, set.pageCount, err)
			}
		}
	}
	if err := c.writePageToCache(ctx, set, true); err != nil {
		t.Fatalf("%s: final page: %v", table, err)
	}
	return req
}

// readTestResult reads a cached result back the way a cache hit does, and counts the rows it streams.
// The reader streams pages from several goroutines at once, so the count is atomic.
func readTestResult(c *QueryCache, written *CacheRequest) (int, error) {
	indexItem := &IndexItem{Key: written.resultKeyRoot, PageCount: written.pageCount}
	var streamed atomic.Int64
	reader := newCacheResultSubscriber(c, indexItem, &CacheRequest{CallId: written.CallId + "-read"}, func(*sdkproto.Row) { streamed.Add(1) })
	err := reader.waitUntilDone(context.Background())
	return int(streamed.Load()), err
}

// Creating a query cache must not allocate its limit, or any fixed size, up front.
func TestNewQueryCache_AllocatesNothingUpFront(t *testing.T) {
	for _, maxSizeMb := range []int{0, 250} {
		before := liveHeap()
		c := newTestQueryCache(t, maxSizeMb, time.Hour)
		grew := liveHeap() - before
		runtime.KeepAlive(c)

		if grew > mb {
			t.Errorf("limit %d MB: creating the cache grew the heap by %d MB", maxSizeMb, grew/mb)
		}
	}
}

// All the memory the cache holds, including its own buffers, stays within its limit,
// however much is written to it.
func TestQueryCache_MemoryStaysWithinLimit(t *testing.T) {
	const limitMb = 20
	before := liveHeap()
	c := newTestQueryCache(t, limitMb, time.Hour)

	// 50 results of 2 pages of about 1 MB: 100 MB in all
	for i := 0; i < 50; i++ {
		writeTestResult(t, c, fmt.Sprintf("table_%d", i), 2*rowBufferSize, 1024)
	}
	grew := liveHeap() - before
	runtime.KeepAlive(c)

	if grew > (limitMb+2)*mb {
		t.Fatalf("after writing 100 MB to a %d MB cache, the cache holds %d MB", limitMb, grew/mb)
	}
}

// When every cached result has expired, the cache gives its memory back,
// even if nothing reads or writes the cache after that.
func TestQueryCache_ExpiredResultsFreeTheirMemory(t *testing.T) {
	const ttl = 200 * time.Millisecond
	before := liveHeap()
	c := newTestQueryCache(t, 0, ttl)

	// 15 results of 2 pages of about 1 MB: 30 MB in all
	for i := 0; i < 15; i++ {
		writeTestResult(t, c, fmt.Sprintf("table_%d", i), 2*rowBufferSize, 1024)
	}

	deadline := time.Now().Add(ttl + 3*time.Second)
	for {
		held := liveHeap() - before
		if held <= 2*mb {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("3 s after every result expired, the idle cache still holds %d MB", held/mb)
		}
		time.Sleep(100 * time.Millisecond)
	}
	runtime.KeepAlive(c)
}

// A cached result is read back whole, or it is a cache miss that streams no rows.
// A miss after some rows were streamed sends those rows twice, because the plugin
// then fetches the whole result again.
func TestQueryCache_CachedResultIsReadWholeOrNotAtAll(t *testing.T) {
	c := newTestQueryCache(t, 10, time.Hour)

	// 80 results of 4 pages of about 100 KB (3,500 rows): 28 MB through a 10 MB cache
	const results, rows = 80, 3*rowBufferSize + rowBufferSize/2
	written := make([]*CacheRequest, results)
	for i := range written {
		written[i] = writeTestResult(t, c, fmt.Sprintf("table_%d", i), rows, 100)
	}

	whole, missed := 0, 0
	for _, w := range written {
		streamed, err := readTestResult(c, w)
		switch {
		case err == nil && streamed == rows:
			whole++
		case IsCacheMiss(err) && streamed == 0:
			missed++
		case IsCacheMiss(err):
			t.Errorf("%s: cache miss after streaming %d of its %d rows", w.Table, streamed, rows)
		default:
			t.Errorf("%s: streamed %d of %d rows, error %v", w.Table, streamed, rows, err)
		}
	}
	if whole == 0 || missed == 0 {
		t.Fatalf("want some results cached and some evicted, got %d whole and %d missed", whole, missed)
	}
}

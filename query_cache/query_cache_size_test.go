package query_cache

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/eko/gocache/lib/v4/store"
	"github.com/turbot/steampipe-plugin-sdk/v6/grpc"
)

// Creating the cache used to allocate ~320MB of heap (bigcache's default preallocation
// plus its index maps), whatever the size limit.
func TestNewQueryCache_HeapCost(t *testing.T) {
	for _, maxSizeMb := range []int{0, 250, 1000} {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		qc, err := NewQueryCache("test", map[string]*grpc.PluginSchema{}, &QueryCacheOptions{Enabled: true, Ttl: 5 * time.Minute, MaxSizeMb: maxSizeMb})
		if err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&after)
		grew := int64(after.HeapAlloc) - int64(before.HeapAlloc)
		if grew > 16<<20 {
			t.Errorf("max size %dMb: creating the cache grew the heap by %dMB, want < 16MB", maxSizeMb, grew>>20)
		}
		runtime.KeepAlive(qc)
	}
}

// A page bigger than the old 7.8MB shard limit must still be cached.
// (A 1000-row page of aws_rds_db_parameter_group is bigger than that.)
func TestQueryCache_LargePageFits(t *testing.T) {
	qc, err := NewQueryCache("test", map[string]*grpc.PluginSchema{}, &QueryCacheOptions{Enabled: true, Ttl: 5 * time.Minute, MaxSizeMb: 250})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	page := bytes.Repeat([]byte("x"), 20<<20)
	if err := qc.cache.Set(ctx, "page", page, store.WithExpiration(time.Minute)); err != nil {
		t.Fatalf("writing a 20MB page failed: %v", err)
	}
	got, err := qc.cache.Get(ctx, "page")
	if err != nil {
		t.Fatalf("reading the page failed: %v", err)
	}
	if !bytes.Equal(got, page) {
		t.Fatal("page read back differs")
	}
}

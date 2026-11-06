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

func TestCacheShardCount(t *testing.T) {
	tests := []struct {
		maxSizeMb int
		shards    int
	}{
		{maxSizeMb: 0, shards: unlimitedCacheShards},
		{maxSizeMb: -1, shards: unlimitedCacheShards},
		{maxSizeMb: 10, shards: 1},
		{maxSizeMb: 64, shards: 1},
		{maxSizeMb: 127, shards: 1},
		{maxSizeMb: 128, shards: 2},
		{maxSizeMb: 250, shards: 2},
		{maxSizeMb: 256, shards: 4},
		{maxSizeMb: 500, shards: 4},
		{maxSizeMb: 1000, shards: 8},
		{maxSizeMb: 1024, shards: 16},
		{maxSizeMb: 100000, shards: 1024},
	}
	for _, tt := range tests {
		if got := cacheShardCount(tt.maxSizeMb); got != tt.shards {
			t.Errorf("cacheShardCount(%d) = %d, want %d", tt.maxSizeMb, got, tt.shards)
		}
	}
}

func TestCacheConfig_SmallInitialAllocation(t *testing.T) {
	for _, maxSizeMb := range []int{0, 10, 250, 1000, 100000} {
		c := cacheConfig(maxSizeMb, 5*time.Minute)
		initialBytes := c.MaxEntriesInWindow * c.MaxEntrySize
		if initialBytes > 8<<20 {
			t.Errorf("max size %dMb: bigcache would preallocate %d bytes, want <= 8MiB", maxSizeMb, initialBytes)
		}
		if maxSizeMb > 0 && c.HardMaxCacheSize != maxSizeMb {
			t.Errorf("max size %dMb: HardMaxCacheSize = %d", maxSizeMb, c.HardMaxCacheSize)
		}
		if want := min(maxSizeMb, minShardSizeMb); maxSizeMb > 0 && c.HardMaxCacheSize/c.Shards < want {
			t.Errorf("max size %dMb: shard size %dMb is below %dMb", maxSizeMb, c.HardMaxCacheSize/c.Shards, want)
		}
	}
}

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

// The size limit is still enforced: writing more than the limit evicts the oldest data.
func TestQueryCache_SizeLimitHolds(t *testing.T) {
	const maxSizeMb = 64
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	qc, err := NewQueryCache("test", map[string]*grpc.PluginSchema{}, &QueryCacheOptions{Enabled: true, Ttl: 5 * time.Minute, MaxSizeMb: maxSizeMb})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	page := bytes.Repeat([]byte("y"), 1<<20)
	for i := 0; i < 4*maxSizeMb; i++ {
		key := "page-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		if err := qc.cache.Set(ctx, key, page, store.WithExpiration(time.Minute)); err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	grew := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if grew > 2*maxSizeMb<<20 {
		t.Errorf("heap grew by %dMB after writing %dMB into a %dMB cache", grew>>20, 4*maxSizeMb, maxSizeMb)
	}
	runtime.KeepAlive(qc)
}

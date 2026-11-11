package query_cache

import "sync/atomic"

type CacheStats struct {
	// keep count of hits and misses. Concurrent queries update them, so they are atomic
	Hits   atomic.Int64
	Misses atomic.Int64
}

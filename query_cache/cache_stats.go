package query_cache

type CacheStats struct {
	// keep count of hits and misses
	Hits   int
	Misses int
}

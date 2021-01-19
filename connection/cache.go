package connection

import (
	"time"

	"github.com/dgraph-io/ristretto"
)

// simple cache implemented using ristretto cache library
type Cache struct {
	cache *ristretto.Cache
}

func NewCache(config *ristretto.Config) *Cache {
	if config == nil {
		config = &ristretto.Config{
			NumCounters: 1e7,     // number of keys to track frequency of (10M).
			MaxCost:     1 << 30, // maximum cost of cache (1GB).
			BufferItems: 64,      // number of keys per Get buffer.
		}
	}
	cache, err := ristretto.NewCache(config)
	if err != nil {
		panic(err)
	}
	return &Cache{cache}
}

func (cache *Cache) Set(key string, value interface{}) {
	ttl := 1 * time.Hour
	cache.cache.SetWithTTL(key, value, 1, ttl)
}

func (cache *Cache) Get(key string) (interface{}, bool) {
	return cache.cache.Get(key)
}

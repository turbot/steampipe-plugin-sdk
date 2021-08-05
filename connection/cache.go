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

func (cache *Cache) Set(key string, value interface{}) bool {
	return cache.SetWithTTL(key, value, 1*time.Hour)
}

func (cache *Cache) SetWithTTL(key string, value interface{}, ttl time.Duration) bool {
	res := cache.cache.SetWithTTL(key, value, 1, ttl)
	// wait for value to pass through buffers
	time.Sleep(10 * time.Millisecond)
	return res
}

func (cache *Cache) Get(key string) (interface{}, bool) {
	return cache.cache.Get(key)
}

func (cache *Cache) Delete(key string) {
	cache.cache.Del(key)
}

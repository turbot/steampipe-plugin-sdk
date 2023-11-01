package connection

import (
	"context"
	"fmt"
	"github.com/dgraph-io/ristretto"
	"github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/store"
	ristretto_store "github.com/eko/gocache/store/ristretto/v4"
	"log"
	"time"
)

// ConnectionCache is a simple cache wrapper - multiple connections use the same underlying cache (owned by the plugin)
// ConnectionCache modifies the cache keys to include the connection name and uses the underlying shared cache
type ConnectionCache struct {
	connectionName string
	cache          *cache.Cache[any]
}

func NewConnectionCache(connectionName string, maxCost int64) (*ConnectionCache, error) {

	ristrettoCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1000,
		MaxCost:     maxCost,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	ristrettoStore := ristretto_store.NewRistretto(ristrettoCache)
	connectionCacheStore := cache.New[any](ristrettoStore)

	cache := &ConnectionCache{
		connectionName: connectionName,
		cache:          connectionCacheStore,
	}

	log.Printf("[INFO] Created connection cache for connection '%s'", connectionName)

	return cache, nil
}

func (c *ConnectionCache) Set(ctx context.Context, key string, value interface{}) error {
	return c.SetWithTTL(ctx, key, value, 1*time.Hour)
}

func (c *ConnectionCache) SetWithTTL(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	// build a key which includes the connection name
	key = c.buildCacheKey(key)

	err := c.cache.Set(ctx,
		key,
		value,
		// if ttl is zero there is no expiration
		store.WithExpiration(ttl),
	)

	// wait for value to pass through buffers (necessary for ristretto)
	time.Sleep(10 * time.Millisecond)

	if err != nil {
		log.Printf("[WARN] SetWithTTL (connection %s, cache key %s) failed - error %v", c.connectionName, key, err)
	}

	return err
}

func (c *ConnectionCache) Get(ctx context.Context, key string) (interface{}, bool) {
	// build a key which includes the connection name
	key = c.buildCacheKey(key)
	item, err := c.cache.Get(ctx, key)
	success := err == nil

	return item, success
}

func (c *ConnectionCache) Delete(ctx context.Context, key string) {
	// build a key which includes the connection name
	key = c.buildCacheKey(key)

	c.cache.Delete(ctx, key)
}

// Clear deletes all cache items for this connection
func (c *ConnectionCache) Clear(ctx context.Context) error {
	log.Printf("[INFO] ConnectionCache.Clear (%s)", c.connectionName)
	return c.cache.Clear(ctx)
}

func (c *ConnectionCache) buildCacheKey(key string) string {
	return fmt.Sprintf("__connection_cache_key_%s__%s", c.connectionName, key)
}

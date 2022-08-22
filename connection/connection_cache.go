package connection

import (
	"context"
	"fmt"
	"github.com/eko/gocache/v3/cache"
	"github.com/eko/gocache/v3/store"
	"time"
)

// ConnectionCache is a simple cache wrapper - multiple connections use the same underlying cache (owned by the plugin)
// ConnectionCache modifies the cache keys to include the connection name and uses the underlying shared cache
type ConnectionCache struct {
	connectionName string
	cache          *cache.Cache[any]
	// map of cache keys - used to clear cache for a specific connection
	keys map[string]struct{}
}

func NewConnectionCache(connectionName string, connectionCache *cache.Cache[any]) *ConnectionCache {
	return &ConnectionCache{
		connectionName: connectionName,
		cache:          connectionCache,
		keys:           make(map[string]struct{}),
	}
}

func (c *ConnectionCache) Set(ctx context.Context, key string, value interface{}) error {
	return c.SetWithTTL(ctx, key, value, 1*time.Hour)
}

func (c *ConnectionCache) SetWithTTL(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	// build a key which includes the connection name
	key = c.buildCacheKey(key)
	expiration := ttl * time.Second

	err := c.cache.Set(ctx,
		key,
		value,
		store.WithExpiration(expiration),
	)

	// wait for value to pass through buffers (necessary for ristretto)
	time.Sleep(10 * time.Millisecond)

	// add to map of keys
	if err == nil {
		c.keys[key] = struct{}{}
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

	err := c.cache.Delete(ctx, key)
	// remove from map of keys
	if err == nil {
		delete(c.keys, key)
	}
}

// Clear deletes all cache items for this connection
func (c *ConnectionCache) Clear(ctx context.Context) {
	for key := range c.keys {
		c.Delete(ctx, key)
	}
}

func (c *ConnectionCache) buildCacheKey(key string) string {
	return fmt.Sprintf("__connection_cache_key_%s__%s", c.connectionName, key)
}

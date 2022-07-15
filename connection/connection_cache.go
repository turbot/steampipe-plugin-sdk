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
}

func NewConnectionCache(connectionName string, connectionCache *cache.Cache[any]) *ConnectionCache {
	return &ConnectionCache{connectionName, connectionCache}
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
	//time.Sleep(10 * time.Millisecond)
	return err
}

func (c *ConnectionCache) Get(ctx context.Context, key string) (interface{}, error) {
	// build a key which includes the connection name
	key = c.buildCacheKey(key)
	return c.cache.Get(ctx, key)
}

func (c *ConnectionCache) Delete(ctx context.Context, key string) {
	// build a key which includes the connection name
	key = c.buildCacheKey(key)

	c.cache.Delete(ctx, key)
}

func (c *ConnectionCache) buildCacheKey(key string) string {
	return fmt.Sprintf("__connection_cache_key_%s__%s", c.connectionName, key)
}

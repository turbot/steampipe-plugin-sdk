package connection

import (
	"context"
	"time"
)

// deprecated
// use ConnectionCache
type Cache struct {
	connectionCache *ConnectionCache
}

func NewCache(connectionCache *ConnectionCache) *Cache {

	return &Cache{connectionCache}
}

func (c *Cache) Set(key string, value interface{}) bool {
	err := c.connectionCache.Set(context.Background(), key, value)
	return err != nil
}

func (c *Cache) SetWithTTL(key string, value interface{}, ttl time.Duration) bool {
	err := c.connectionCache.SetWithTTL(context.Background(), key, value, ttl)
	return err != nil
}

func (c *Cache) Get(key string) (interface{}, bool) {
	return c.connectionCache.Get(context.Background(), key)
}

func (c *Cache) Delete(key string) {
	c.connectionCache.Delete(context.Background(), key)
}

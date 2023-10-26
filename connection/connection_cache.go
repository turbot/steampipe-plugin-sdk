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
	//keys           map[string]struct{}
	//keysLock       sync.Mutex
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
		//keys:           make(map[string]struct{}),
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
	//c.keysLock.Lock()
	//c.keys[key] = struct{}{}
	//c.keysLock.Unlock()

	//log.Printf("[INFO] SetWithTTL (connection %s, cache key %s) ", c.connectionName, key)
	err := c.cache.Set(ctx,
		key,
		value,
		// if ttl is zero there is no expiration
		store.WithExpiration(ttl),
		//// put connection name in tags
		//store.WithTags([]string{c.connectionName}),
	)

	// wait for value to pass through buffers (necessary for ristretto)
	time.Sleep(10 * time.Millisecond)

	if err != nil {
		log.Printf("[WARN] SetWithTTL (connection %s, cache key %s) failed - error %v", c.connectionName, key, err)
	}
	// TACTICAL
	// verify this key has been set with the correct tag
	//var foundKeyForTag bool
	//var cacheKeys []string
	//tagKey := fmt.Sprintf("gocache_tag_%s", c.connectionName)
	//if result, err := c.cache.Get(ctx, tagKey); err == nil {
	//	if bytes, ok := result.([]byte); ok {
	//		cacheKeys = strings.Split(string(bytes), ",")
	//		for _, k := range cacheKeys {
	//			if k == key {
	//				foundKeyForTag = true
	//				break
	//			}
	//		}
	//	}
	//}
	//if !foundKeyForTag {
	//	log.Printf("[WARN] SetWithTTL (connection %s, cache key %s) - key NOT found for tag %s", c.connectionName, key, c.connectionName)
	//}

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

	//// read tags keys from ristretto and verify they exist
	//var cacheKeys []string
	//tagKey := fmt.Sprintf("gocache_tag_%s", c.connectionName)
	//if result, err := c.cache.Get(ctx, tagKey); err == nil {
	//	log.Printf("[INFO] existing cache key for connection '%s': %s ", c.connectionName, result)
	//
	//	if bytes, ok := result.([]byte); ok {
	//		cacheKeys = strings.Split(string(bytes), ",")
	//		for _, k := range cacheKeys {
	//			_, err := c.cache.Get(ctx, k)
	//			if err != nil {
	//				log.Printf("[INFO] %s  does not exists in cache", k)
	//			}
	//		}
	//	}
	//}
	//
	////c.cache.Invalidate(ctx, store.WithInvalidateTags([]string{c.connectionName}))
	////
	////// now verify the tags have been deleted
	////for _, k := range cacheKeys {
	////	_, err := c.cache.Get(ctx, k)
	////	if err == nil {
	////		log.Printf("[INFO]  cache key for connection '%s' : %s  still exists in cache after clear", c.connectionName, k)
	////	}
	////}
	////
	////TODO TACTICAL
	//c.keysLock.Lock()
	//log.Printf("[INFO] ConnectionCache clear for connection '%s', deleting %d %s",
	//	c.connectionName,
	//	len(c.keys),
	//	pluralize.NewClient().Pluralize("key", len(c.keys), false),
	//)
	//for key := range c.keys {
	//	//_, err := c.cache.Get(ctx, key)
	//	//log.Printf("[INFO] before delete get %s returns err %v", key, err)
	//	c.cache.Delete(ctx, key)
	//	//time.Sleep(10 * time.Millisecond)
	//	//_, err = c.cache.Get(ctx, key)
	//	//log.Printf("[INFO] after delete get %s returns %v ", key, err)
	//}
	//c.keys = make(map[string]struct{})
	//c.keysLock.Unlock()

}

func (c *ConnectionCache) buildCacheKey(key string) string {
	return fmt.Sprintf("__connection_cache_key_%s__%s", c.connectionName, key)
}

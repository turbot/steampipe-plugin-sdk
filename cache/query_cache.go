package cache

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
)

// TODO do not use unsafe quals use quals map and  remove key column qual logic

// default ttl - increase this if any client has a larger ttl
const defaultTTL = 5 * time.Minute

type QueryCache struct {
	cache           *ristretto.Cache
	Stats           *CacheStats
	connectionName  string
	PluginSchema    map[string]*proto.TableSchema
	pendingData     map[string]*pendingIndexBucket
	pendingDataLock sync.Mutex
	ttlLock         sync.Mutex
	ttl             time.Duration
}

type CacheStats struct {
	// keep count of hits and misses
	Hits   int
	Misses int
}

func NewQueryCache(connectionName string, pluginSchema map[string]*proto.TableSchema) (*QueryCache, error) {
	cache := &QueryCache{
		Stats:          &CacheStats{},
		connectionName: connectionName,
		PluginSchema:   pluginSchema,
		pendingData:    make(map[string]*pendingIndexBucket),
		ttl:            defaultTTL,
	}

	config := &ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
		Metrics:     true,
	}
	var err error
	if cache.cache, err = ristretto.NewCache(config); err != nil {
		return nil, err
	}
	log.Printf("[INFO] query cache created")
	return cache, nil
}

func (c *QueryCache) Set(table string, qualMap map[string]*proto.Quals, columns []string, limit int64, result *QueryCacheResult) (res bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] QueryCache Set suffered a panic: %s", helpers.ToError(r))
			res = false
		}

		// clear the corresponding pending item - we have completed the transfer
		// (we need to do this even if the cache set fails)
		c.pendingItemComplete(table, qualMap, columns, limit)
	}()
	cacheQualMap := c.buildCacheQualMap(table, qualMap)

	// get ttl - read here in case the property is updated  between the 2 uses below
	ttl := c.ttl

	// if any data was returned, extract the columns from the first row
	if len(result.Rows) > 0 {
		for col := range result.Rows[0].Columns {
			if !helpers.StringSliceContains(columns, col) {
				columns = append(columns, col)
			}
		}
	}
	sort.Strings(columns)
	log.Printf("[TRACE] QueryCache Set - connectionName: %s, table: %s, columns: %s, limit %d\n", c.connectionName, table, columns, limit)
	defer log.Printf("[TRACE] QueryCache Set() DONE")

	// write to the result cache
	// set the insertion time
	resultKey := c.buildResultKey(table, cacheQualMap, columns, limit)
	// estimate cost at 8 bytes per column value
	cost := len(result.Rows) * len(columns) * 8
	log.Printf("[TRACE] cache item cost = %d (%d rows, %d columns)", cost, len(result.Rows), len(columns))
	c.cache.SetWithTTL(resultKey, result, int64(cost), ttl)

	// now update the index
	// get the index bucket for this table and quals
	indexBucketKey := c.buildIndexKey(c.connectionName, table)
	indexBucket, ok := c.getIndexBucket(indexBucketKey)
	indexItem := NewIndexItem(columns, resultKey, limit, cacheQualMap)
	if !ok {
		// create new index bucket
		indexBucket = newIndexBucket()
	}
	indexBucket.Append(indexItem)

	if res := c.cache.SetWithTTL(indexBucketKey, indexBucket, 1, ttl); !res {
		log.Printf("[WARN] cache Set failed")
		return res
	}

	// wait for value to pass through cache buffers
	time.Sleep(10 * time.Millisecond)
	c.logMetrics()

	return true
}

// CancelPendingItem cancels a pending item - called when an execute call fails for any reason
func (c *QueryCache) CancelPendingItem(table string, qualMap map[string]*proto.Quals, columns []string, limit int64) {
	log.Printf("[TRACE] QueryCache CancelPendingItem table: %s", table)
	// clear the corresponding pending item
	c.pendingItemComplete(table, qualMap, columns, limit)
}

func (c *QueryCache) Get(ctx context.Context, table string, qualMap map[string]*proto.Quals, columns []string, limit, clientTTLSeconds int64) *QueryCacheResult {
	clientTTL := time.Duration(clientTTLSeconds) * time.Second
	// if the client TTL is greater than the cache TTL, update the cache value to match the client value
	// lock to handle concurrent updates
	c.ttlLock.Lock()
	if clientTTL > c.ttl {
		log.Printf("[INFO] QueryCache.Get %p client TTL %s is greater than cache TTL %s - updating cache value", c, clientTTL.String(), c.ttl.String())
		c.ttl = clientTTL
	}
	c.ttlLock.Unlock()

	// get the index bucket for this table and quals
	// - this contains cache keys for all cache entries for specified table and quals
	indexBucketKey := c.buildIndexKey(c.connectionName, table)

	log.Printf("[TRACE] QueryCache Get - indexBucketKey %s, quals", indexBucketKey)

	// build a map containing only the quals which we use for building a cache key (i.e. key column quals)
	cacheQualMap := c.buildCacheQualMap(table, qualMap)

	// do we have a cached result?
	res := c.getCachedResult(indexBucketKey, table, cacheQualMap, columns, limit, clientTTLSeconds)
	if res != nil {
		log.Printf("[INFO] CACHE HIT")
		// cache hit!
		return res
	}

	// there was no cached result - is there data fetch in progress?
	if pendingItem := c.getPendingResultItem(indexBucketKey, table, cacheQualMap, columns, limit); pendingItem != nil {
		log.Printf("[TRACE] found pending item - waiting for it")
		// so there is a pending result, wait for it
		return c.waitForPendingItem(ctx, pendingItem, indexBucketKey, table, cacheQualMap, columns, limit, clientTTLSeconds)
	}

	log.Printf("[INFO] CACHE MISS")
	// cache miss
	return nil
}

func (c *QueryCache) buildCacheQualMap(table string, qualMap map[string]*proto.Quals) map[string]*proto.Quals {
	keyColumns := c.getKeyColumnsForTable(table)

	cacheQualMap := make(map[string]*proto.Quals)
	for col, quals := range qualMap {
		log.Printf("[TRACE] buildCacheQualMap col %s, quals %+v", col, quals)
		// if this column is a key column, include in key
		if _, ok := keyColumns[col]; ok {
			log.Printf("[TRACE] INCLUDING COLUMN")
			cacheQualMap[col] = quals
		} else {
			log.Printf("[TRACE] EXCLUDING COLUMN")
		}
	}
	return cacheQualMap
}

func (c *QueryCache) Clear() {
	c.cache.Clear()
}

func (c *QueryCache) getCachedResult(indexBucketKey, table string, qualMap map[string]*proto.Quals, columns []string, limit, ttlSeconds int64) *QueryCacheResult {
	keyColumns := c.getKeyColumnsForTable(table)

	log.Printf("[TRACE] QueryCache getCachedResult - index bucket key: %s ttlSeconds %d\n", indexBucketKey, ttlSeconds)
	indexBucket, ok := c.getIndexBucket(indexBucketKey)
	if !ok {
		c.Stats.Misses++
		log.Printf("[TRACE] getCachedResult - no index bucket")
		return nil
	}

	// now check whether we have a cache entry that covers the required quals and columns - check the index
	indexItem := indexBucket.Get(qualMap, columns, limit, ttlSeconds, keyColumns)
	if indexItem == nil {
		limitString := "NONE"
		if limit != -1 {
			limitString = fmt.Sprintf("%d", limit)
		}
		c.Stats.Misses++
		log.Printf("[TRACE] getCachedResult - no cached data covers columns %v, limit %s\n", columns, limitString)
		return nil
	}

	// so we have a cache index, retrieve the item
	result, ok := c.getResult(indexItem.Key)
	if !ok {
		c.Stats.Misses++
		log.Printf("[TRACE] getCachedResult - no item retrieved for cache key %s", indexItem.Key)
		return nil
	}

	c.Stats.Hits++

	return result
}

// GetIndex retrieves an index bucket for a given cache key
func (c *QueryCache) getIndexBucket(indexKey string) (*IndexBucket, bool) {
	result, ok := c.cache.Get(indexKey)
	if !ok {
		return nil, false
	}
	return result.(*IndexBucket), true
}

// GetResult retrieves a query result for a given cache key
func (c *QueryCache) getResult(resultKey string) (*QueryCacheResult, bool) {
	result, ok := c.cache.Get(resultKey)
	if !ok {
		return nil, false
	}
	return result.(*QueryCacheResult), true
}

func (c *QueryCache) buildIndexKey(connectionName, table string) string {
	str := c.sanitiseKey(fmt.Sprintf("index__%s%s",
		connectionName,
		table))
	return str
}

func (c *QueryCache) buildResultKey(table string, qualMap map[string]*proto.Quals, columns []string, limit int64) string {
	str := c.sanitiseKey(fmt.Sprintf("%s%s%s%s%d",
		c.connectionName,
		table,
		c.formatQualMapForKey(table, qualMap),
		strings.Join(columns, ","),
		limit))
	return str
}

func (c *QueryCache) formatQualMapForKey(table string, qualMap map[string]*proto.Quals) string {
	if len(qualMap) == 0 {
		return ""
	}

	var strs = make([]string, len(qualMap))
	// first build list of keys, then sort them
	keys := make([]string, len(qualMap))
	idx := 0
	for key := range qualMap {
		keys[idx] = key
		idx++
	}
	sort.Strings(keys)
	log.Printf("[TRACE] formatQualMapForKey sorted keys %v\n", keys)

	// now construct cache key from ordered quals
	for i, key := range keys {
		for _, q := range qualMap[key].Quals {
			strs = append(strs, fmt.Sprintf("%s-%s-%v", q.FieldName, q.GetStringValue(), grpc.GetQualValue(q.Value)))
		}
		strs[i] = strings.Join(strs, "-")
	}
	return strings.Join(strs, "-")
}

// return a map of key column for the given table
func (c *QueryCache) getKeyColumnsForTable(table string) map[string]*proto.KeyColumn {
	res := make(map[string]*proto.KeyColumn)
	// build a list of all key columns
	tableSchema, ok := c.PluginSchema[table]
	if ok {
		for _, k := range append(tableSchema.ListCallKeyColumnList, tableSchema.GetCallKeyColumnList...) {
			res[k.Name] = k
		}
	}
	return res
}

func (c *QueryCache) sanitiseKey(str string) string {
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\t", "", -1)
	return str
}

func (c *QueryCache) logMetrics() {
	log.Printf("[TRACE] ------------------------------------ ")
	log.Printf("[TRACE] Cache Metrics ")
	log.Printf("[TRACE] ------------------------------------ ")
	log.Printf("[TRACE] MaxCost: %d", c.cache.MaxCost)
	log.Printf("[TRACE] KeysAdded: %d", c.cache.Metrics.KeysAdded())
	log.Printf("[TRACE] CostAdded: %d", c.cache.Metrics.CostAdded())
	log.Printf("[TRACE] KeysEvicted: %d", c.cache.Metrics.KeysEvicted())
	log.Printf("[TRACE] CostEvicted: %d", c.cache.Metrics.CostEvicted())
	log.Printf("[TRACE] ------------------------------------ ")
}

package cache

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/turbot/go-kit/helpers"
	typeHelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

const PendingQueryTimeoutEnvVar = "STEAMPIPE_CACHE_PENDING_QUERY_TIMEOUT"

// TODO do not use unsafe quals use quals map and  remove key column qual logic

// insert all data with this fixed large ttl - each client may specify its own ttl requirements when reading the cache
const ttl = 24 * time.Hour

type QueryCache struct {
	cache               *ristretto.Cache
	Stats               *CacheStats
	connectionName      string
	PluginSchema        map[string]*proto.TableSchema
	pendingData         map[string]*pendingIndexBucket
	pendingDataLock     sync.Mutex
	pendingQueryTimeout time.Duration
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
	}

	config := &ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	}
	var err error
	if cache.cache, err = ristretto.NewCache(config); err != nil {
		return nil, err
	}
	log.Printf("[INFO] query cache created")

	cache.pendingQueryTimeout = 90 * time.Second
	if timeout, ok := os.LookupEnv(PendingQueryTimeoutEnvVar); ok {
		if timeoutSecs, err := typeHelpers.ToInt64(timeout); err != nil {
			cache.pendingQueryTimeout = time.Duration(timeoutSecs) * time.Second
		}
	}
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

	// if any data was returned, extract the columns from the first row
	if len(result.Rows) > 0 {
		for col := range result.Rows[0].Columns {
			if !helpers.StringSliceContains(columns, col) {
				columns = append(columns, col)
			}
		}
	}
	sort.Strings(columns)
	log.Printf("[TRACE] QueryCache Set - connectionName: %s, table: %s, columns: %s\n", c.connectionName, table, columns)
	defer log.Printf("[TRACE] QueryCache Set() DONE")

	// write to the result cache
	// set the insertion time
	result.InsertionTime = time.Now()
	resultKey := c.buildResultKey(table, qualMap, columns)
	c.cache.SetWithTTL(resultKey, result, 1, ttl)

	// now update the index
	// get the index bucket for this table and quals
	indexBucketKey := c.buildIndexKey(c.connectionName, table, qualMap)
	indexBucket, ok := c.getIndexBucket(indexBucketKey)

	log.Printf("[TRACE] index key %s, result key %s", indexBucketKey, resultKey)

	if ok {
		indexBucket.Append(&IndexItem{columns, resultKey, limit})
	} else {
		// create new index bucket
		indexBucket = newIndexBucket().Append(NewIndexItem(columns, resultKey, limit))
	}
	if res := c.cache.SetWithTTL(indexBucketKey, indexBucket, 1, ttl); !res {
		log.Printf("[TRACE] Set failed")
		return res
	}

	// wait for value to pass through cache buffers
	time.Sleep(10 * time.Millisecond)

	return true
}

// CancelPendingItem cancels a pending item - called when an execute call fails for any reason
func (c *QueryCache) CancelPendingItem(table string, qualMap map[string]*proto.Quals, columns []string, limit int64) {
	log.Printf("[Trace] QueryCache CancelPendingItem %s", table)
	// clear the corresponding pending item
	c.pendingItemComplete(table, qualMap, columns, limit)
}

func (c *QueryCache) Get(ctx context.Context, table string, qualMap map[string]*proto.Quals, columns []string, limit, ttlSeconds int64) *QueryCacheResult {
	// get the index bucket for this table and quals
	// - this contains cache keys for all cache entries for specified table and quals
	indexBucketKey := c.buildIndexKey(c.connectionName, table, qualMap)

	log.Printf("[TRACE] QueryCache Get - indexBucketKey %s", indexBucketKey)

	// do we have a cached result?
	res := c.getCachedResult(indexBucketKey, columns, limit, ttlSeconds)
	if res != nil {
		log.Printf("[INFO] CACHE HIT")
		// cache hit!
		return res
	}

	// there was no cached result - is there data fetch in progress?
	if pendingItem := c.getPendingResultItem(indexBucketKey, table, qualMap, columns, limit); pendingItem != nil {
		log.Printf("[INFO] found pending item - waiting for it")
		// so there is a pending result, wait for it
		return c.waitForPendingItem(ctx, pendingItem, indexBucketKey, table, qualMap, columns, limit, ttlSeconds)
	}

	log.Printf("[INFO] CACHE MISS")
	// cache miss
	return nil
}

func (c *QueryCache) Clear() {
	c.cache.Clear()
}

func (c *QueryCache) getCachedResult(indexBucketKey string, columns []string, limit int64, ttlSeconds int64) *QueryCacheResult {
	log.Printf("[TRACE] QueryCache getCachedResult - index bucket key: %s\n", indexBucketKey)
	indexBucket, ok := c.getIndexBucket(indexBucketKey)
	if !ok {
		c.Stats.Misses++
		log.Printf("[TRACE] getCachedResult - no index bucket")
		return nil
	}

	// now check whether we have a cache entry that covers the required columns - check the index
	indexItem := indexBucket.Get(columns, limit)
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
	if time.Since(result.InsertionTime) > time.Duration(ttlSeconds)*time.Second {
		c.Stats.Misses++
		log.Printf("[TRACE] cache ttl %d has expired", ttlSeconds)
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

func (c *QueryCache) buildIndexKey(connectionName, table string, qualMap map[string]*proto.Quals) string {
	str := c.sanitiseKey(fmt.Sprintf("index__%s%s%s",
		connectionName,
		table,
		c.formatQualMapForKey(table, qualMap)))
	return str
}

func (c *QueryCache) buildResultKey(table string, qualMap map[string]*proto.Quals, columns []string) string {
	str := c.sanitiseKey(fmt.Sprintf("%s%s%s%s",
		c.connectionName,
		table,
		c.formatQualMapForKey(table, qualMap),
		strings.Join(columns, ",")))
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

	// get a predicate function which tells us whether to include a qual
	shouldIncludeQualInKey := c.getShouldIncludeQualInKey(table)

	for i, key := range keys {
		strs[i] = c.formatQualsForKey(qualMap[key], shouldIncludeQualInKey)
	}
	return strings.Join(strs, "-")
}

func (c *QueryCache) formatQualsForKey(quals *proto.Quals, shouldIncludeQualInKey func(string) bool) string {
	var strs []string
	for _, q := range quals.Quals {
		if shouldIncludeQualInKey(q.FieldName) {
			strs = append(strs, fmt.Sprintf("%s-%s-%v", q.FieldName, q.GetStringValue(), grpc.GetQualValue(q.Value)))
		}
	}
	return strings.Join(strs, "-")
}

// only include key column quals and optional quals
func (c *QueryCache) getShouldIncludeQualInKey(table string) func(string) bool {

	// build a list of all key columns
	tableSchema, ok := c.PluginSchema[table]
	if !ok {
		// any errors, just default to including the column
		return func(string) bool { return true }
	}
	var cols []string
	for _, k := range tableSchema.ListCallKeyColumnList {
		cols = append(cols, k.Name)
	}
	for _, k := range tableSchema.GetCallKeyColumnList {
		cols = append(cols, k.Name)
	}

	return func(column string) bool {
		res := helpers.StringSliceContains(cols, column)
		log.Printf("[TRACE] shouldIncludeQual, column %s, include = %v", column, res)
		return res
	}

}

func (c *QueryCache) sanitiseKey(str string) string {
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\t", "", -1)
	return str
}

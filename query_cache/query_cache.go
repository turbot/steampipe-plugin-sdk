package query_cache

import (
	"context"
	"fmt"
	"github.com/allegro/bigcache/v3"
	"github.com/eko/gocache/v3/cache"
	"github.com/eko/gocache/v3/store"
	"google.golang.org/protobuf/proto"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v4/telemetry"
)

type CacheData interface {
	proto.Message
	*sdkproto.QueryResult | *sdkproto.IndexBucket
}

// default ttl - increase this if any client has a larger ttl
const defaultTTL = 5 * time.Minute

type CacheMissError struct{}

func (CacheMissError) Error() string { return "cache miss" }

func IsCacheMiss(err error) bool {
	if err == nil {
		return false
	}
	// BigCache returns "Entry not found"
	errorStrings := []string{CacheMissError{}.Error(), "Entry not found"}
	return helpers.StringSliceContains(errorStrings, err.Error())
}

type CacheStats struct {
	// keep count of hits and misses
	Hits   int
	Misses int
}

const rowBufferSize = 10000

type CacheRequest struct {
	CallId         string
	Table          string
	QualMap        map[string]*sdkproto.Quals
	Columns        []string
	Limit          int64
	ConnectionName string
	Rows           []*sdkproto.Row
	TtlSeconds     int64
}

type QueryCache struct {
	Stats      *CacheStats
	pluginName string
	// map of connection name to plugin schema
	PluginSchemaMap map[string]*grpc.PluginSchema
	// map of pending cache transfers, keyed by index bucket key
	pendingData     map[string]*pendingIndexBucket
	pendingDataLock sync.Mutex
	ttlLock         sync.Mutex
	ttl             time.Duration
	streamLock      sync.Mutex
	cache           *cache.Cache[[]byte]
	// map of ongoing set requests, keyed by callId
	setRequests map[string]*CacheRequest
	setLock     sync.Mutex
}

func NewQueryCache(pluginName string, pluginSchemaMap map[string]*grpc.PluginSchema, maxCacheStorageMb int) (*QueryCache, error) {

	queryCache := &QueryCache{
		Stats:           &CacheStats{},
		pluginName:      pluginName,
		PluginSchemaMap: pluginSchemaMap,
		pendingData:     make(map[string]*pendingIndexBucket),
		setRequests:     make(map[string]*CacheRequest),
		ttl:             defaultTTL,
	}
	if err := queryCache.createCache(maxCacheStorageMb); err != nil {
		return nil, err
	}
	log.Printf("[INFO] query cache created")
	return queryCache, nil
}

func (c *QueryCache) createCache(maxCacheStorageMb int) error {
	cacheStore, err := c.createCacheStore(maxCacheStorageMb)
	if err != nil {
		return err
	}
	c.cache = cache.New[[]byte](cacheStore)
	return nil
}

func (c *QueryCache) createCacheStore(maxCacheStorageMb int) (store.StoreInterface, error) {
	config := bigcache.DefaultConfig(5 * time.Minute)
	log.Printf("[TRACE] createCacheStore for plugin '%s' setting max size to %dMb", c.pluginName, maxCacheStorageMb)
	//config.Shards = 10
	// max entry size is HardMaxCacheSize/1000
	//config.MaxEntrySize = (maxCacheStorageMb) * 1024 * 1024

	bigcacheClient, _ := bigcache.NewBigCache(config)
	bigcacheStore := store.NewBigcache(bigcacheClient)
	return bigcacheStore, nil
}

func (c *QueryCache) Get(ctx context.Context, req *CacheRequest) (*sdkproto.QueryResult, error) {
	ctx, span := telemetry.StartSpan(ctx, "QueryCache.Get (%s)", req.Table)
	defer func() {
		//span.SetAttributes(attribute.Bool("cache-hit", cacheHit))
		span.End()
	}()

	// if the client TTL is greater than the cache TTL, update the cache value to match the client value
	c.setTtl(req.TtlSeconds)

	// get the index bucket for this table and quals
	// - this contains cache keys for all cache entries for specified table and quals
	indexBucketKey := c.buildIndexKey(req.ConnectionName, req.Table)

	log.Printf("[TRACE] QueryCache Get - indexBucketKey %s, quals", indexBucketKey)

	// do we have a cached result?
	result, err := c.getCachedQueryResult(ctx, indexBucketKey, req)

	if IsCacheMiss(err) {
		log.Printf("[TRACE] getCachedQueryResult returned CACHE MISS - checking for pending transfers")
		// there was no cached result - is there data fetch in progress?
		if pendingItem := c.getPendingResultItem(indexBucketKey, req); pendingItem != nil {
			log.Printf("[TRACE] found pending item - waiting for it")
			// so there is a pending result, wait for it
			return c.waitForPendingItem(ctx, pendingItem, indexBucketKey, req)
		}
	}
	return result, err
}

func (c *QueryCache) setTtl(clientTTLSeconds int64) {
	clientTTL := time.Duration(clientTTLSeconds) * time.Second
	// lock to handle concurrent updates
	c.ttlLock.Lock()
	if clientTTL > c.ttl {
		log.Printf("[INFO] QueryCache.Get %p client TTL %s is greater than cache TTL %s - updating cache value", c, clientTTL.String(), c.ttl.String())
		c.ttl = clientTTL
	}
	c.ttlLock.Unlock()
}

func (c *QueryCache) StartSet(ctx context.Context, req *CacheRequest) {
	log.Printf("[WARN] StartSet %s", req.CallId)
	c.setLock.Lock()
	defer c.setLock.Unlock()

	req.Rows = make([]*sdkproto.Row, 0, rowBufferSize)
	//req.count = 0
	//req.resultKeyRoot = c.buildResultKey(req)
	c.setRequests[req.CallId] = req
}

func (c *QueryCache) IterateSet(row *sdkproto.Row, callId string) error {
	c.setLock.Lock()
	// get the ongoing request
	req, ok := c.setRequests[callId]
	c.setLock.Unlock()
	if !ok {
		return fmt.Errorf("IterateSet called for callId %s but there is no in progress set operation", callId)
	}
	req.Rows = append(req.Rows, row)

	return nil
}

func (c *QueryCache) EndSet(ctx context.Context, callId string) (err error) {
	log.Printf("[WARN] EndSet %s", callId)
	c.setLock.Lock()
	defer c.setLock.Unlock()
	// get the ongoing request
	req, ok := c.setRequests[callId]
	if !ok {
		log.Printf("[WARN] EndSet called for callId %s but there is no in progress set operation", callId)
		return fmt.Errorf("EndSet called for callId %s but there is no in progress set operation", callId)
	}
	// remove entry from the map
	defer delete(c.setRequests, callId)

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] QueryCache EndSet suffered a panic: %v", helpers.ToError(r))
			err = helpers.ToError(r)
		}

		// clear the corresponding pending item - we have completed the transfer
		// (we need to do this even if the cache set fails)
		c.pendingItemComplete(req)
	}()

	log.Printf("[WARN] QueryCache EndSet - CacheCommand_SET_RESULT_END command executed")

	// get ttl - read here in case the property is updated  between the 2 uses below
	ttl := c.ttl

	// write to the result cache¡
	resultKey := c.buildResultKey(req)
	result := &sdkproto.QueryResult{Rows: req.Rows}
	err = doSet(ctx, resultKey, result, ttl, c.cache)
	if err != nil {
		log.Printf("[WARN] QueryCache EndSet - result Set failed: %v", err)
		return err
	} else {
		log.Printf("[WARN] QueryCache EndSet - result written")
	}

	// now update the index
	// get the index bucket for this table and quals
	indexBucketKey := c.buildIndexKey(req.ConnectionName, req.Table)
	indexBucket, err := c.getCachedIndexBucket(ctx, indexBucketKey)
	if err != nil {
		if IsCacheMiss(err) {
			log.Printf("[WARN] getCachedIndexBucket returned cache miss")
		} else {
			// if there is an error fetching the index bucket, log it and return
			// we do not want to risk overwriting an existing index bucketan
			log.Printf("[WARN] getCachedIndexBucket failed: %v", err)
			return
		}
	}

	indexItem := NewIndexItem(req.Columns, resultKey, req.Limit, req.QualMap)
	if indexBucket == nil {
		// create new index bucket
		indexBucket = newIndexBucket()
	}
	indexBucket.Append(indexItem)
	log.Printf("[WARN] QueryCache EndSet - Added index item to bucket, key %s", resultKey)
	err = c.cacheSetIndexBucket(ctx, indexBucketKey, indexBucket, ttl)

	if err != nil {
		log.Printf("[WARN] cache Set failed for index bucket: %v", err)
	} else {
		log.Printf("[WARN] QueryCache EndSet - IndexBucket written")
	}

	return err
}

func (c *QueryCache) AbortSet(callId string) {
	c.setLock.Lock()
	defer c.setLock.Unlock()
	// remove pending item
	delete(c.setRequests, callId)
}

// CancelPendingItem cancels a pending item - called when an execute call fails for any reason
func (c *QueryCache) CancelPendingItem(req *CacheRequest) {
	log.Printf("[TRACE] QueryCache CancelPendingItem table: %s", req.Table)
	// clear the corresponding pending item
	c.pendingItemComplete(req)
}

func (c *QueryCache) getCachedIndexBucket(ctx context.Context, key string) (*IndexBucket, error) {

	var indexBucket = &sdkproto.IndexBucket{}
	if err := doGet(ctx, key, c.cache, indexBucket); err != nil {
		if IsCacheMiss(err) {
			c.Stats.Misses++
			log.Printf("[TRACE] getCachedIndexBucket - no item retrieved for cache key %s", key)
		} else {
			log.Printf("[WARN] cacheGetResult Get failed %v", err)
		}
		return nil, err
	}

	log.Printf("[TRACE] getCachedIndexBucket cache hit ")
	var res = IndexBucketfromProto(indexBucket)
	return res, nil
}

// try to fetch cached data for the given cache request
func (c *QueryCache) getCachedQueryResult(ctx context.Context, indexBucketKey string, req *CacheRequest) (*sdkproto.QueryResult, error) {
	log.Printf("[TRACE] QueryCache getCachedQueryResult - table %s, connectionName %s", req.Table, req.ConnectionName)
	keyColumns := c.getKeyColumnsForTable(req.Table, req.ConnectionName)

	log.Printf("[TRACE] QueryCache getCachedQueryResult - index bucket key: %s ttlSeconds %d\n", indexBucketKey, req.TtlSeconds)
	indexBucket, err := c.getCachedIndexBucket(ctx, indexBucketKey)
	if err != nil {
		return nil, err
	}

	// now check whether we have a cache entry that covers the required quals and columns - check the index
	indexItem := indexBucket.Get(req, keyColumns)
	if indexItem == nil {
		limitString := "NONE"
		if req.Limit != -1 {
			limitString = fmt.Sprintf("%d", req.Limit)
		}
		c.Stats.Misses++
		log.Printf("[WARN] getCachedQueryResult - no cached data covers columns %v, limit %s\n", req.Columns, limitString)
		return nil, new(CacheMissError)
	}

	// so we have a cache index, retrieve the item
	log.Printf("[TRACE] fetch result from cache, key: '%s'", indexItem.Key)

	var cacheResult = &sdkproto.QueryResult{}
	if err := doGet[*sdkproto.QueryResult](ctx, indexItem.Key, c.cache, cacheResult); err != nil {
		if IsCacheMiss(err) {
			c.Stats.Misses++
			log.Printf("[TRACE] getCachedQueryResult - no item retrieved for cache key %s", indexItem.Key)
		} else {
			log.Printf("[WARN] cacheGetResult Get failed %v", err)
		}
		return nil, err
	}

	c.Stats.Hits++

	return cacheResult, nil
}

func (c *QueryCache) buildIndexKey(connectionName, table string) string {
	str := c.sanitiseKey(fmt.Sprintf("index__%s%s",
		connectionName,
		table))
	return str
}

func (c *QueryCache) buildResultKey(req *CacheRequest) string {

	str := c.sanitiseKey(fmt.Sprintf("%s%s%s%s%d",
		req.ConnectionName,
		req.Table,
		c.formatQualMapForKey(req.QualMap),
		strings.Join(req.Columns, ","),
		req.Limit))
	return str
}

func (c *QueryCache) formatQualMapForKey(qualMap map[string]*sdkproto.Quals) string {
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
func (c *QueryCache) getKeyColumnsForTable(table string, connectionName string) map[string]*sdkproto.KeyColumn {
	res := make(map[string]*sdkproto.KeyColumn)
	// build a list of all key columns
	tableSchema, ok := c.PluginSchemaMap[connectionName].Schema[table]
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

func (c *QueryCache) cacheSetIndexBucket(ctx context.Context, key string, indexBucket *IndexBucket, ttl time.Duration) error {
	log.Printf("[TRACE] cacheSetIndexBucket %s", key)

	return doSet(ctx, key, indexBucket.AsProto(), ttl, c.cache)
}
func (c *QueryCache) cacheSetResult(ctx context.Context, key string, result *sdkproto.QueryResult, ttl time.Duration, callId string) error {
	return doSet(ctx, key, result, ttl, c.cache)
}

func doGet[T CacheData](ctx context.Context, key string, cache *cache.Cache[[]byte], target T) error {
	// get the bytes from the cache
	getRes, err := cache.Get(ctx, key)
	if err != nil {
		if IsCacheMiss(err) {
			log.Printf("[TRACE] doGet cache miss ")
		} else {
			log.Printf("[WARN] cache.Get returned error %s", err.Error())
		}
		//  return the error
		return err
	}

	// unmarshall into the correct type
	err = proto.Unmarshal(getRes, target)
	if err != nil {
		log.Printf("[WARN] error unmarshalling result: %s", err.Error())
		return err
	}

	return nil
}

func doSet[T CacheData](ctx context.Context, key string, value T, ttl time.Duration, cache *cache.Cache[[]byte]) error {
	bytes, err := proto.Marshal(value)
	if err != nil {
		log.Printf("[WARN] doSet - marshal failed: %v", err)
		return err
	}

	err = cache.Set(ctx,
		key,
		bytes,
		store.WithExpiration(ttl),
	)
	if err != nil {
		log.Printf("[WARN] doSet failed: %v", err)
	}
	return err
}

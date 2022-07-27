package query_cache

import (
	"context"
	"fmt"
	"github.com/allegro/bigcache/v3"
	"github.com/eko/gocache/v3/cache"
	"github.com/eko/gocache/v3/store"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v4/error_helpers"
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v4/telemetry"
)

type CacheData interface {
	proto.Message
	*sdkproto.QueryResult | *sdkproto.IndexBucket
}

// default ttl - increase this if any client has a larger ttl
const (
	defaultTTL    = 5 * time.Minute
	rowBufferSize = 1000
)

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
	setRequests       map[string]*CacheRequest
	setRequestMapLock sync.Mutex
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

func (c *QueryCache) Get(ctx context.Context, req *CacheRequest, streamRowFunc func(row *sdkproto.Row)) error {
	cacheHit := false
	ctx, span := telemetry.StartSpan(ctx, "QueryCache.Get (%s)", req.Table)
	defer func() {
		span.SetAttributes(attribute.Bool("cache-hit", cacheHit))
		span.End()
	}()

	// set root result key
	req.resultKeyRoot = c.buildResultKey(req)

	// if the client TTL is greater than the cache TTL, update the cache value to match the client value
	c.setTtl(req.TtlSeconds)

	// get the index bucket key for this table and quals
	indexBucketKey := c.buildIndexKey(req.ConnectionName, req.Table)
	log.Printf("[TRACE] QueryCache Get - indexBucketKey %s, quals", indexBucketKey)

	// do we have a cached result?
	_, err := c.getCachedQueryResult(ctx, indexBucketKey, req, streamRowFunc)
	if err == nil {
		// only set cache hit if there was no error
		cacheHit = true
	} else if IsCacheMiss(err) {
		log.Printf("[TRACE] getCachedQueryResult returned CACHE MISS - checking for pending transfers")
		// there was no cached result - is there data fetch in progress?q
		if pendingItem := c.getPendingResultItem(indexBucketKey, req); pendingItem != nil {
			log.Printf("[TRACE] found pending item - waiting for it")
			// so there is a pending result, wait for it
			return c.waitForPendingItem(ctx, pendingItem, indexBucketKey, req, streamRowFunc)
		}
		log.Printf("[WARN] CACHE MISS ")
	}

	return err
}

func (c *QueryCache) StartSet(_ context.Context, req *CacheRequest) {
	log.Printf("[WARN] StartSet %s", req.CallId)

	// set root result key
	req.resultKeyRoot = c.buildResultKey(req)
	// create rows buffer
	req.rows = make([]*sdkproto.Row, rowBufferSize)

	c.setRequestMapLock.Lock()
	c.setRequests[req.CallId] = req
	c.setRequestMapLock.Unlock()
}

func (c *QueryCache) IterateSet(ctx context.Context, row *sdkproto.Row, callId string) error {
	c.setRequestMapLock.Lock()
	// get the ongoing request
	req, ok := c.setRequests[callId]
	c.setRequestMapLock.Unlock()

	if !ok {
		return fmt.Errorf("IterateSet called for callId %s but there is no in progress set operation", callId)
	}
	// was there an error in a previous iterate
	if req.err != nil {
		return req.err
	}

	// acquire rowlock - this is to ensure prev page has been written before we overwrite the buffer
	//req.rowLock.Lock()
	req.rows[req.rowIndex] = row
	req.rowIndex++
	//req.rowLock.Unlock()

	if req.rowIndex == rowBufferSize {
		// reset index and update page count
		log.Printf("[WARN] IterateSet written 1 page of %d rows. Page count %d", rowBufferSize, req.pageCount)
		//go func() {
		// lock row lock - to ensure the row buffer is not overwritten before we write to cache
		//req.rowLock.Lock()
		//defer req.rowLock.Unlock()

		req.err = c.writePageToCache(ctx, req)

		//}()
	}

	return nil
}

func (c *QueryCache) EndSet(ctx context.Context, callId string) (err error) {
	log.Printf("[WARN] QueryCache EndSet %s", callId)
	c.setRequestMapLock.Lock()
	defer c.setRequestMapLock.Unlock()
	// get the ongoing request
	req, ok := c.setRequests[callId]
	if !ok {
		log.Printf("[WARN] EndSet called for callId %s but there is no in progress set operation", callId)
		return fmt.Errorf("EndSet called for callId %s but there is no in progress set operation", callId)
	}

	// lock the rowlock to ensure any previous writes are complete
	//req.rowLock.Lock()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] QueryCache EndSet suffered a panic: %v", helpers.ToError(r))
			err = helpers.ToError(r)
		}
		// clear the rowlock
		//req.rowLock.Unlock()

		// remove entry from the map
		delete(c.setRequests, callId)
		// clear the corresponding pending item - we have completed the transfer
		// (we need to do this even if the cache set fails)
		c.pendingItemComplete(req)
	}()

	// write the remainder to the result cache
	err = c.writePageToCache(ctx, req)
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

	indexItem := NewIndexItem(req)

	if indexBucket == nil {
		// create new index bucket
		indexBucket = newIndexBucket()
	}
	indexBucket.Append(indexItem)
	log.Printf("[WARN] QueryCache EndSet - Added index item to bucket, page count %d,  key %s", req.pageCount, req.resultKeyRoot)

	// write index bucket back to cache
	err = c.cacheSetIndexBucket(ctx, indexBucketKey, indexBucket, req.ttl())
	indexBucket, err = c.getCachedIndexBucket(ctx, indexBucketKey)

	if err != nil {
		log.Printf("[WARN] cache Set failed for index bucket: %v", err)
	} else {
		log.Printf("[WARN] QueryCache EndSet - IndexBucket written")
	}

	return err
}

func (c *QueryCache) AbortSet(ctx context.Context, callId string) {
	c.setRequestMapLock.Lock()
	defer c.setRequestMapLock.Unlock()
	// get the ongoing request
	req, ok := c.setRequests[callId]
	if !ok {
		return
	}

	// clear the corresponding pending item
	log.Printf("[WARN] QueryCache AbortSet table: %s, cancelling pending item", req.Table)
	c.pendingItemComplete(req)

	log.Printf("[WARN] QueryCache AbortSet - deleting %d pages from the cache", req.pageCount)
	// remove all pages that have already been written
	for i := int64(0); i < req.pageCount; i++ {
		pageKey := getPageKey(req.resultKeyRoot, i)
		c.cache.Delete(ctx, pageKey)
	}

	// remove pending item
	delete(c.setRequests, callId)
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

// write a page of rows to the cache
func (c *QueryCache) writePageToCache(ctx context.Context, req *CacheRequest) error {
	// ask the request for it's currently buffered rows
	rows := req.getRows()
	if len(rows) == 0 {
		return nil
	}

	pageKey := req.getPageResultKey()

	log.Printf("[WARN] QueryCache writePageToCache: %d rows, page key %s", len(rows), pageKey)
	// write to cache - construct result key
	result := &sdkproto.QueryResult{Rows: rows}
	err := doSet(ctx, pageKey, result, req.ttl(), c.cache)
	if err != nil {
		log.Printf("[WARN] writePageToCache cache Set failed: %v", err)
	} else {
		log.Printf("[WARN] writePageToCache Set - result written")
	}

	// reset the row buffer index and increment the page count
	req.rowIndex = 0
	req.pageCount++

	return err
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
// this is factored into a separate function from Get to support fetching pending results with same code
func (c *QueryCache) getCachedQueryResult(ctx context.Context, indexBucketKey string, req *CacheRequest, streamRowFunc func(row *sdkproto.Row)) (int64, error) {
	log.Printf("[WARN] QueryCache getCachedQueryResult - table %s, connectionName %s", req.Table, req.ConnectionName)
	keyColumns := c.getKeyColumnsForTable(req.Table, req.ConnectionName)

	log.Printf("[WARN] index bucket key: %s ttlSeconds %d limit: %d\n", indexBucketKey, req.TtlSeconds, req.Limit)
	indexBucket, err := c.getCachedIndexBucket(ctx, indexBucketKey)
	if err != nil {
		return 0, err
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
		return 0, new(CacheMissError)
	}

	// so we have a cache index, retrieve the item
	log.Printf("[WARN] got an index item - try to retrieve rows from cache")

	var cachedRowsFetched int64 = 0
	cacheHit := true
	var errors []error
	errorChan := make(chan (error), indexItem.PageCount)

	var wg sync.WaitGroup
	const maxReadThreads = 5
	var sem = semaphore.NewWeighted(maxReadThreads)
	log.Printf("[WARN] %d pages", indexItem.PageCount)

	// ok so we have an index item - we now stream
	for pageIdx := int64(0); pageIdx < indexItem.PageCount; pageIdx++ {
		sem.Acquire(ctx, 1)
		wg.Add(1)
		// construct the page key, _using the index item key as the root_
		p := getPageKey(indexItem.Key, pageIdx)

		go func(pageKey string) {
			defer wg.Done()
			defer sem.Release(1)

			log.Printf("[WARN] fetching key: %s", pageKey)
			var cacheResult = &sdkproto.QueryResult{}
			if err := doGet[*sdkproto.QueryResult](ctx, pageKey, c.cache, cacheResult); err != nil {
				if IsCacheMiss(err) {
					log.Printf("[WARN] getCachedQueryResult - no item retrieved for cache key %s", pageKey)
				} else {
					log.Printf("[WARN] cacheGetResult Get failed %v", err)
				}
				errorChan <- err
				return
			}

			log.Printf("[WARN] got result: %d rows", len(cacheResult.Rows))

			for _, r := range cacheResult.Rows {
				// check for context cancellation
				if error_helpers.IsCancelled(ctx) {
					log.Printf("[WARN] getCachedQueryResult context cancelled - returning")
					return
				}
				atomic.AddInt64(&cachedRowsFetched, 1)
				streamRowFunc(r)
			}
		}(p)
	}
	doneChan := make(chan bool)
	go func() {
		wg.Wait()
		close(doneChan)
	}()

	for {
		select {
		case err := <-errorChan:
			log.Printf("[WARN] received error: %s", err.Error())
			if IsCacheMiss(err) {
				cacheHit = false
			} else {
				errors = append(errors, err)
			}
		case <-doneChan:
			log.Printf("[WARN] CACHE GET DONE, STREAMING NULL ROW")
			// now stream a nil row to indicate completion
			streamRowFunc(nil)

			// any real errors return them
			if len(errors) > 0 {
				return 0, helpers.CombineErrors(errors...)
			}
			if cacheHit {
				// this was a hit - return
				log.Printf("[WARN] CACHE HIT")
				c.Stats.Hits++
				return cachedRowsFetched, nil
			} else {
				log.Printf("[WARN] CACHE MISS")
				c.Stats.Misses++
				return 0, CacheMissError{}
			}
		}
	}
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

// write index bucket back to cache
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
		log.Printf("[WARN] doSet cache.Set failed: %v", err)
	}

	return err
}

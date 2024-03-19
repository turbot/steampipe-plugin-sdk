package query_cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/store"
	bigcache_store "github.com/eko/gocache/store/bigcache/v4"
	"github.com/gertd/go-pluralize"
	"github.com/sethvargo/go-retry"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
)

type CacheData interface {
	proto.Message
	*sdkproto.QueryResult | *sdkproto.IndexBucket
}

const (
	// cache has a default hard TTL limit of 24 hours
	DefaultMaxTtl = 24 * time.Hour
	// the number of rows we buffer before writing a page the cache
	rowBufferSize = 1000
)

type QueryCache struct {
	Stats      *CacheStats
	pluginName string
	// map of connection name to plugin schema
	PluginSchemaMap map[string]*grpc.PluginSchema
	// map of pending cache transfers, keyed by index bucket key
	pendingData     map[string]*pendingIndexBucket
	pendingDataLock sync.RWMutex

	cache *cache.Cache[[]byte]
	// map of ongoing set requests, keyed by callId
	setRequests       map[string]*setRequest
	setRequestMapLock sync.RWMutex
	Enabled           bool
}

func NewQueryCache(pluginName string, pluginSchemaMap map[string]*grpc.PluginSchema, opts *QueryCacheOptions) (*QueryCache, error) {
	queryCache := &QueryCache{
		Stats:           &CacheStats{},
		pluginName:      pluginName,
		PluginSchemaMap: pluginSchemaMap,
		pendingData:     make(map[string]*pendingIndexBucket),
		setRequests:     make(map[string]*setRequest),
		Enabled:         opts.Enabled,
	}
	if err := queryCache.createCache(opts.MaxSizeMb, opts.Ttl); err != nil {
		return nil, err
	}
	log.Printf("[INFO] query cache created, max size %dMb", opts.MaxSizeMb)
	return queryCache, nil
}

func (c *QueryCache) createCache(maxCacheStorageMb int, maxTtl time.Duration) error {
	cacheStore, err := c.createCacheStore(maxCacheStorageMb, maxTtl)
	if err != nil {
		return err
	}
	c.cache = cache.New[[]byte](cacheStore)
	return nil
}

func (c *QueryCache) createCacheStore(maxCacheStorageMb int, maxTtl time.Duration) (store.StoreInterface, error) {
	config := bigcache.DefaultConfig(maxTtl)
	// ensure each shard is at least 5Mb
	config.Shards = 1024
	for maxCacheStorageMb/config.Shards < 5 {
		config.Shards = config.Shards / 2
		if config.Shards == 2 {
			break
		}
	}
	config.HardMaxCacheSize = maxCacheStorageMb
	log.Printf("[INFO] createCacheStore for plugin '%s' setting max size to %dMb, Shards: %d, max shard size: %d ", c.pluginName, maxCacheStorageMb, config.Shards, ((maxCacheStorageMb*1024*1024)/config.Shards)/(1024*1024))

	bigcacheClient, _ := bigcache.New(context.Background(), config)
	bigcacheStore := bigcache_store.NewBigcache(bigcacheClient)
	return bigcacheStore, nil
}

func (c *QueryCache) Get(ctx context.Context, req *CacheRequest, streamUncachedRowFunc, streamCachedRowFunc func(row *sdkproto.Row)) error {
	cacheHit := false
	ctx, span := telemetry.StartSpan(ctx, "QueryCache.Get (%s)", req.Table)
	defer func() {
		span.SetAttributes(attribute.Bool("cache-hit", cacheHit))
		span.End()
	}()

	// set root result key
	req.resultKeyRoot = c.buildResultKey(req)

	// get the index bucket key for this table and quals
	indexBucketKey := c.buildIndexKey(req.ConnectionName, req.Table)
	log.Printf("[INFO] QueryCache Get - indexBucketKey %s, quals: %s (%s)", indexBucketKey, req.CallId, grpc.QualMapToLogLine(req.QualMap))

	// do we have a cached result?
	resultSubscriber, err := c.getCachedQueryResult(ctx, indexBucketKey, req, streamCachedRowFunc)
	if err == nil {
		log.Printf("[INFO] subscribed to cache result request")
		// wait for all rows to be streamed (or an error)
		err = resultSubscriber.waitUntilDone(ctx)
		if err == nil {
			// Success!
			log.Printf("[INFO] All rows streamed (%s)", req.CallId)
			cacheHit = true
			return nil
		}

		// fall through to return error
		log.Printf("[WARN] waiting for all cached data failed: %s (%s)", err.Error(), req.CallId)
	}

	// if there IS an error which is NOT a cache miss, just return the error
	if !IsCacheMiss(err) {
		return err
	}

	// so we have a cache miss

	// there was no cached result - is there data fetch in progress?
	// If so, subscribe to it (will return a subscriber - or a subscription error if it failed)
	// If not, create one and subscribe to it (will return a cache miss error)
	subscriber, err := c.findAndSubscribeToPendingRequest(ctx, indexBucketKey, req, streamUncachedRowFunc, streamCachedRowFunc)
	if err == nil {
		log.Printf("[TRACE] subscribed to pending request")
		// wait for all rows to be streamed (or an error)
		err = subscriber.waitUntilDone()
		if err != nil {
			log.Printf("[WARN] waiting for all subscription data failed: %s (%s)", err.Error(), req.CallId)
		} else {
			log.Printf("[INFO] All rows streamed (%s)", req.CallId)
			cacheHit = true
		}
		// fall through to return error
	}

	return err
}

func (c *QueryCache) findAndSubscribeToPendingRequest(ctx context.Context, indexBucketKey string, req *CacheRequest, streamUncachedRowFunc, streamCachedRowFunc func(row *sdkproto.Row)) (subscriber *setRequestSubscriber, err error) {
	log.Printf("[INFO] getCachedQueryResult returned CACHE MISS - checking for pending transfers (%s)", req.CallId)

	// try to get pending item within a read lock
	c.pendingDataLock.RLock()
	pendingItem := c.getPendingResultItem(indexBucketKey, req)
	c.pendingDataLock.RUnlock()

	if pendingItem != nil {
		// so we have a pending item - subscribe to it
		log.Printf("[INFO] found pending item [%s] - subscribing to its data (%s)", pendingItem.callId, req.CallId)
		return c.subscribeToPendingRequest(ctx, pendingItem.pendingSetRequest, req, streamCachedRowFunc)
	}

	// get a write lock in preparation for adding a pending item
	c.pendingDataLock.Lock()

	//  before adding a pending result, try again for a pending item inside the write lock
	// this is to allow for the race condition where 2 threads are both making a concurrent cache request
	// - one will create a pending item first
	if pendingItem := c.getPendingResultItem(indexBucketKey, req); pendingItem != nil {
		// release the writ lock before subscribeToPendingRequest
		c.pendingDataLock.Unlock()

		// ok NOW there is a pending item - just subscribe to it, returning any error
		return c.subscribeToPendingRequest(ctx, pendingItem.pendingSetRequest, req, streamCachedRowFunc)
	}

	// so there is still no pending item :(

	// add a pending result so anyone else asking for this data will wait the fetch to complete
	// NOTE: pass streamUncachedRowFunc so we do not count these in cached results
	c.addPendingResult(ctx, indexBucketKey, req, streamUncachedRowFunc)

	// unlock the write lock
	c.pendingDataLock.Unlock()

	//  return cache miss error
	//  NOTE: we DO NOT return the subscriber - calling code needs to do the data fetch and stream into the cache
	return nil, CacheMissError{}
}

func (c *QueryCache) subscribeToPendingRequest(ctx context.Context, pendingSetRequest *setRequest, req *CacheRequest, streamRowFunc func(row *sdkproto.Row)) (subscriber *setRequestSubscriber, err error) {
	log.Printf("[TRACE] subscribeToPendingRequest table %s (%s)", req.Table, req.CallId)

	// create a subscriber
	subscriber = newSetRequestSubscriber(streamRowFunc, req.CallId, req.StreamContext, pendingSetRequest)

	// subscribe to setRequest
	pendingSetRequest.requestLock.Lock()
	pendingSetRequest.subscribe(subscriber)
	pendingSetRequest.requestLock.Unlock()

	// tell subscriber to start the async read thread
	subscriber.readRowsAsync(ctx)

	return subscriber, err
}

// startSet begins a streaming cache Set operation.
// NOTE: this mutates req
func (c *QueryCache) startSet(ctx context.Context, req *CacheRequest, streamRowFunc func(row *sdkproto.Row)) *setRequest {
	log.Printf("[INFO] startSet table: %s (%s)", req.Table, req.CallId)

	// set root result key
	req.resultKeyRoot = c.buildResultKey(req)

	// create a set request
	setRequest := newSetRequest(req, c)

	// subscribe to the set request, so that all data streamed to cache is streamed to us
	// NOTE: we subscribe to the cache rather than streaming the result directly to GRPC as we  need to
	// decouple the reading of the data (by Postgres) and the writing if the scan rows into the cache
	// if we do not do this, writing to the cache can be blocked if postgres stops reading rows for the initial scan
	// NOTE: ignore error as subscribeToPendingRequest can only fail when
	// the set request has buffered data already which we fail to copy
	// that cannot happen in this case
	_, _ = c.subscribeToPendingRequest(ctx, setRequest, req, streamRowFunc)

	// lock the set request map
	c.setRequestMapLock.Lock()
	c.setRequests[req.CallId] = setRequest
	c.setRequestMapLock.Unlock()

	return setRequest
}

func (c *QueryCache) IterateSet(ctx context.Context, row *sdkproto.Row, callId string) error {
	// get the ongoing request
	c.setRequestMapLock.RLock()
	req, ok := c.setRequests[callId]
	c.setRequestMapLock.RUnlock()
	if !ok {
		// not expected
		return fmt.Errorf("IterateSet called for callId %s but there is no in-progress 'set' operation", callId)
	}

	// add row to the request page buffer
	if err := req.addRow(row); err != nil {
		return err
	}

	log.Printf("[TRACE] IterateSet rowCount %d", req.rowCount)

	// if we have buffered a page, write to cache
	if req.bufferIndex == rowBufferSize {
		// reset index and update page count
		log.Printf("[TRACE] IterateSet writing 1 page of %d rows. Page count %d (%s)", rowBufferSize, req.pageCount, req.CallId)
		req.err = c.writePageToCache(ctx, req, false)
	}

	return nil
}

func (c *QueryCache) EndSet(ctx context.Context, callId string) (err error) {
	log.Printf("[INFO] EndSet (%s)", callId)

	c.setRequestMapLock.RLock()
	// get the ongoing request
	req, ok := c.setRequests[callId]
	c.setRequestMapLock.RUnlock()
	if !ok {
		log.Printf("[WARN] EndSet called for callId %s but there is no in progress set operation", callId)
		return fmt.Errorf("EndSet called for callId %s but there is no in progress set operation", callId)
	}

	// ensure we delete set request from map
	defer func() {
		// remove entry from the map
		c.setRequestMapLock.Lock()
		delete(c.setRequests, callId)
		c.setRequestMapLock.Unlock()

		// clear the corresponding pending item - we have completed the transfer
		// (we need to do this even if the cache set fails)
		log.Printf("[TRACE] QueryCache EndSet table: %s, marking pending item complete (%s)", req.Table, req.CallId)
		log.Printf("[INFO] calling pendingItemComplete (%s)", callId)
		c.pendingItemComplete(req.CacheRequest)

		// wait for all subscribers to complete
		// this about errors
		req.waitForSubscribers(ctx)
		log.Printf("[INFO] EndSet table %s %d rows (%s)", req.Table, req.rowCount, callId)
	}()

	log.Printf("[TRACE] EndSet (%s) table %s root key %s, pages: %d", callId, req.Table, req.resultKeyRoot, req.pageCount)

	// write the remaining buffered rows to the cache
	err = c.writePageToCache(ctx, req, true)
	if err != nil {
		log.Printf("[WARN] QueryCache EndSet - result Set failed: %v", err)
		return err
	}

	// now update the cache index
	err = c.updateIndex(ctx, callId, req)
	if err != nil {
		return err
	}

	return err
}

func (c *QueryCache) AbortSet(ctx context.Context, callId string, err error) {
	c.setRequestMapLock.Lock()
	// get the ongoing request
	req, ok := c.setRequests[callId]
	// remove set request item
	delete(c.setRequests, callId)
	c.setRequestMapLock.Unlock()
	if !ok {
		return
	}

	// set request state to error - all subsctribers will see the state and give up
	req.requestLock.Lock()
	log.Printf("[WARN] QueryCache AbortSet - aborting request  with error %s (%d %s) (%s)",
		err.Error(),
		len(req.subscribers),
		pluralize.NewClient().Pluralize("subscriber", len(req.subscribers), false),
		req.CallId)

	req.state = requestError
	req.err = err
	req.requestLock.Unlock()

	log.Printf("[INFO] QueryCache AbortSet pendingItemComplete (%s)", req.CallId)
	// clear the corresponding pending item
	c.pendingItemComplete(req.CacheRequest)

	log.Printf("[INFO] QueryCache AbortSet - deleting %d pages from the cache (%s)", req.pageCount, req.CallId)
	// remove all pages that have already been written
	for i := 0; i < int(req.pageCount); i++ {
		pageKey := getPageKey(req.resultKeyRoot, i)
		c.cache.Delete(ctx, pageKey)
	}
	log.Printf("[INFO] QueryCache AbortSet done (%s)", req.CallId)
}

// ClearForConnection removes all cache entries for the given connection
func (c *QueryCache) ClearForConnection(ctx context.Context, connectionName string) error {
	return c.cache.Invalidate(ctx, store.WithInvalidateTags([]string{connectionName}))
}

func (c *QueryCache) updateIndex(ctx context.Context, callId string, req *setRequest) error {
	// get the index bucket for this table and connection
	indexBucketKey := c.buildIndexKey(req.ConnectionName, req.Table)
	log.Printf("[INFO] QueryCache EndSet UpdateIndex indexBucketKey %s", indexBucketKey)

	indexBucket, err := c.getCachedIndexBucket(ctx, indexBucketKey)
	if err != nil {
		if !IsCacheMiss(err) {
			// if there is an error fetching the index bucket, log it and return
			// we do not want to risk overwriting an existing index bucket
			log.Printf("[WARN] getCachedIndexBucket failed: %v", err)
			return nil
		}

		log.Printf("[INFO] getCachedIndexBucket returned cache miss (%s)", callId)
	}

	indexItem := NewIndexItem(req.CacheRequest)
	// create new index bucket if needed
	if indexBucket == nil {
		indexBucket = newIndexBucket()
	}
	indexBucket.Append(indexItem)
	log.Printf("[INFO] QueryCache EndSet - Added index item to bucket, row count: %d, table: %s, quals: %s, bucket items: %d (%s)", req.rowCount, req.Table, grpc.QualMapToLogLine(req.QualMap), len(indexBucket.Items), callId)

	// write index bucket back to cache
	err = c.cacheSetIndexBucket(ctx, indexBucketKey, indexBucket, req.CacheRequest)
	if err != nil {
		log.Printf("[WARN] cache Set failed for index bucket: %v", err)
	} else {
		log.Printf("[TRACE] QueryCache EndSet - IndexBucket written (%s)", callId)
	}
	return err
}

// write a page of rows to the cache
func (c *QueryCache) writePageToCache(ctx context.Context, req *setRequest, finalPage bool) error {
	// if the request only has 1 subscriber, wait for it to read all buffered data
	req.requestLock.RLock()
	subscribers := maps.Keys(req.subscribers)
	req.requestLock.RUnlock()
	if len(subscribers) == 1 {
		for subscriber := range req.subscribers {
			// TODO should we timeout?
			_ = retry.Do(ctx, retry.NewConstant(50*time.Millisecond), func(ctx context.Context) error {
				log.Printf("[WARN] writePageToCache - checking subscriber has read all rows %v (%s)", subscribers, req.CallId)
				if subscriber.allAvailableRowsStreamed(req.rowCount) {
					return nil
				}
				return retry.RetryableError(errors.New("subscriber has not read all rows"))
			})
		}
	}

	// now lock the request
	req.requestLock.Lock()

	// ask the request for it's currently buffered rows
	rows := req.getBufferedRows()

	// reset the row buffer index and increment the page count
	// (BEFORE building pageKey)
	req.bufferIndex = 0
	req.pageCount++

	// set completion state if this is last page
	if finalPage {
		req.state = requestComplete
	}
	req.requestLock.Unlock()

	// build a cache key for this page
	pageKey := req.getPageResultKey()

	log.Printf("[TRACE] QueryCache writePageToCache: %d rows, pageCount %d, page key %s (%s)", len(rows), req.pageCount, pageKey, req.CallId)
	// write to cache - construct result key
	result := &sdkproto.QueryResult{Rows: rows}

	// put connection name in tags
	tags := []string{req.ConnectionName}
	err := doSet(ctx, pageKey, result, req.ttl(), c.cache, tags)
	if err != nil {
		log.Printf("[WARN] writePageToCache cache Set failed: %v - page key %s (%s)", err, pageKey, req.CallId)
	} else {
		log.Printf("[TRACE] writePageToCache Set - result written (%s)", req.CallId)
	}

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

func (c *QueryCache) getCachedQueryResult(ctx context.Context, indexBucketKey string, req *CacheRequest, streamRowFunc func(row *sdkproto.Row)) (*cacheResultSubscriber, error) {
	log.Printf("[INFO] QueryCache getCachedQueryResult - table %s, connectionName %s (%s)", req.Table, req.ConnectionName, req.CallId)
	keyColumns := c.getKeyColumnsForTable(req.Table, req.ConnectionName)

	log.Printf("[INFO] index bucket key: %s ttlSeconds %d limit: %d (%s)", indexBucketKey, req.TtlSeconds, req.Limit, req.CallId)
	indexBucket, err := c.getCachedIndexBucket(ctx, indexBucketKey)
	if err != nil {
		log.Printf("[INFO] getCachedQueryResult found no index bucket for table %s (%s)", req.Table, req.CallId)
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
		log.Printf("[INFO] getCachedQueryResult found no index item- no cached data covers columns %v, limit %s (%s)", req.Columns, limitString, req.CallId)
		return nil, new(CacheMissError)
	}

	return newCacheResultSubscriber(c, indexItem, req, streamRowFunc), nil
}

func (c *QueryCache) buildIndexKey(connectionName, table string) string {
	str := c.sanitiseKey(fmt.Sprintf("index__%s_%s",
		connectionName,
		table))
	return str
}

// build a result key, using connection, table, quals, columns and limit
func (c *QueryCache) buildResultKey(req *CacheRequest) string {
	qualString := ""
	if len(req.QualMap) > 0 {
		qualString = fmt.Sprintf("_%s", c.formatQualMapForKey(req.QualMap))
	}
	str := c.sanitiseKey(fmt.Sprintf("%s_%s%s_%s_%d",
		req.ConnectionName,
		req.Table,
		qualString,
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
	schema, ok := c.PluginSchemaMap[connectionName]
	if !ok {
		return res
	}
	// build a list of all key columns
	tableSchema, ok := schema.Schema[table]
	if ok {
		for _, k := range append(tableSchema.ListCallKeyColumnList, tableSchema.GetCallKeyColumnList...) {
			res[k.Name] = k
		}
	} else {
		log.Printf("[TRACE] getKeyColumnsForTable found no schema for table '%s'", table)
	}
	return res
}

func (c *QueryCache) sanitiseKey(str string) string {
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\t", "", -1)
	return str
}

// write index bucket back to cache
func (c *QueryCache) cacheSetIndexBucket(ctx context.Context, indexBucketKey string, indexBucket *IndexBucket, req *CacheRequest) error {
	log.Printf("[TRACE] cacheSetIndexBucket %s", indexBucketKey)

	// put connection name in tags
	tags := []string{req.ConnectionName}
	return doSet(ctx, indexBucketKey, indexBucket.AsProto(), req.ttl(), c.cache, tags)
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

func doSet[T CacheData](ctx context.Context, key string, value T, ttl time.Duration, cache *cache.Cache[[]byte], tags []string) error {
	bytes, err := proto.Marshal(value)
	if err != nil {
		log.Printf("[WARN] doSet - marshal failed: %v", err)
		return err
	}

	err = cache.Set(ctx,
		key,
		bytes,
		store.WithExpiration(ttl),
		store.WithTags(tags),
	)
	if err != nil {
		log.Printf("[WARN] doSet cache.Set failed: %v", err)
	}

	return err
}

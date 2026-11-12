package query_cache

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/grpc"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v6/telemetry"
	"go.opentelemetry.io/otel/attribute"
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
	// map of connection name to plugin schema (accessed atomically for thread safety)
	pluginSchemaMap atomic.Pointer[map[string]*grpc.PluginSchema]
	// map of pending cache transfers, keyed by index bucket key
	pendingData     map[string]*pendingIndexBucket
	pendingDataLock sync.RWMutex

	cache *pageStore
	// map of ongoing set requests, keyed by callId
	setRequests       map[string]*setRequest
	setRequestMapLock sync.RWMutex
	Enabled           bool
	// the number of scans started, used to give each scan its own page keys
	scanCount atomic.Int64
}

func NewQueryCache(pluginName string, pluginSchemaMap map[string]*grpc.PluginSchema, opts *QueryCacheOptions) (*QueryCache, error) {
	queryCache := &QueryCache{
		Stats:       &CacheStats{},
		pluginName:  pluginName,
		pendingData: make(map[string]*pendingIndexBucket),
		setRequests: make(map[string]*setRequest),
		Enabled:     opts.Enabled,
	}
	// Store the schema map atomically
	queryCache.pluginSchemaMap.Store(&pluginSchemaMap)
	if err := queryCache.createCache(opts.MaxSizeMb, opts.Ttl); err != nil {
		return nil, err
	}
	log.Printf("[INFO] query cache created, max size %dMb", opts.MaxSizeMb)
	return queryCache, nil
}

// SetPluginSchemaMap atomically updates the plugin schema map.
// This is safe to call concurrently with cache reads.
func (c *QueryCache) SetPluginSchemaMap(schemaMap map[string]*grpc.PluginSchema) {
	c.pluginSchemaMap.Store(&schemaMap)
}

func (c *QueryCache) createCache(maxCacheStorageMb int, maxTtl time.Duration) error {
	c.cache = newPageStore(maxCacheStorageMb, maxTtl)
	log.Printf("[INFO] query cache for plugin '%s': max size %dMb, max ttl %s", c.pluginName, maxCacheStorageMb, maxTtl)
	return nil
}

func (c *QueryCache) Get(ctx context.Context, req *CacheRequest, streamUncachedRowFunc, streamCachedRowFunc func(row *sdkproto.Row)) error {
	cacheHit := false
	ctx, span := telemetry.StartSpan(ctx, "QueryCache.Get (%s)", "%s", req.Table)
	defer func() {
		span.SetAttributes(attribute.Bool("cache-hit", cacheHit))
		span.End()
	}()

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

func (c *QueryCache) findAndSubscribeToPendingRequest(ctx context.Context, indexBucketKey string, req *CacheRequest, streamUncachedRowFunc, streamCachedRowFunc func(row *sdkproto.Row)) (*setRequestSubscriber, error) {
	log.Printf("[INFO] getCachedQueryResult returned CACHE MISS - checking for pending transfers (%s)", req.CallId)

	// hold the write lock until we have subscribed to a pending item or added one,
	// so 2 concurrent requests for the same data do not both start a scan
	c.pendingDataLock.Lock()
	defer c.pendingDataLock.Unlock()

	pendingItems, _ := c.getPendingItemSatisfyingRequest(indexBucketKey, req)
	for _, pendingItem := range pendingItems {
		log.Printf("[INFO] found pending item [%s] - subscribing to its data (%s)", pendingItem.callId, req.CallId)
		if subscriber, ok := c.subscribeToPendingRequest(ctx, pendingItem.pendingSetRequest, req, streamCachedRowFunc); ok {
			return subscriber, nil
		}
		log.Printf("[INFO] cannot subscribe to pending item [%s]: it has failed, or the cache no longer has its first pages (%s)", pendingItem.callId, req.CallId)
	}

	// so there is no pending item we can subscribe to :(

	// add a pending result so anyone else asking for this data will wait the fetch to complete
	// NOTE: pass streamUncachedRowFunc so we do not count these in cached results
	c.addPendingResult(ctx, indexBucketKey, req, streamUncachedRowFunc)

	//  return cache miss error
	//  NOTE: we DO NOT return the subscriber - calling code needs to do the data fetch and stream into the cache
	return nil, CacheMissError{}
}

// subscribeToPendingRequest subscribes to a scan and starts streaming its rows.
// It returns false if the scan cannot be joined (see setRequest.subscribe).
func (c *QueryCache) subscribeToPendingRequest(ctx context.Context, pendingSetRequest *setRequest, req *CacheRequest, streamRowFunc func(row *sdkproto.Row)) (*setRequestSubscriber, bool) {
	log.Printf("[TRACE] subscribeToPendingRequest table %s (%s)", req.Table, req.CallId)

	// create a subscriber
	subscriber := newSetRequestSubscriber(streamRowFunc, req.CallId, req.StreamContext, pendingSetRequest)

	// subscribe to setRequest
	pendingSetRequest.requestLock.Lock()
	ok := pendingSetRequest.subscribe(subscriber)
	pendingSetRequest.requestLock.Unlock()
	if !ok {
		return nil, false
	}

	// tell subscriber to start the async read thread
	subscriber.readRowsAsync(ctx)

	return subscriber, true
}

// startSet begins a streaming cache Set operation.
// NOTE: this mutates req
func (c *QueryCache) startSet(ctx context.Context, req *CacheRequest, streamRowFunc func(row *sdkproto.Row)) *setRequest {
	log.Printf("[INFO] startSet table: %s (%s)", req.Table, req.CallId)

	// set root result key. Each scan has its own key, so a scan never writes over the pages
	// of an earlier result of the same query, which a query may still be reading
	req.resultKeyRoot = fmt.Sprintf("%s-%d", c.buildResultKey(req), c.scanCount.Add(1))

	// create a set request
	setRequest := newSetRequest(req, c)

	// subscribe to the set request, so that all data streamed to cache is streamed to us
	// NOTE: we subscribe to the cache rather than streaming the result directly to GRPC as we  need to
	// decouple the reading of the data (by Postgres) and the writing if the scan rows into the cache
	// if we do not do this, writing to the cache can be blocked if postgres stops reading rows for the initial scan
	// NOTE: this cannot fail, as the scan has written no pages yet
	setRequest.ownSubscriber, _ = c.subscribeToPendingRequest(ctx, setRequest, req, streamRowFunc)

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
		return c.writePageToCache(ctx, req, false)
	}

	return nil
}

// EndSet writes the last page of the scan, and adds the result to the cache index.
// Then it waits for the scan's readers to finish. If the query that started the scan could
// not read every row, EndSet returns that query's error, so the query fails. A failure to
// cache the result is only logged, as the query still gets its rows.
func (c *QueryCache) EndSet(ctx context.Context, callId string) error {
	log.Printf("[INFO] EndSet (%s)", callId)

	c.setRequestMapLock.RLock()
	// get the ongoing request
	req, ok := c.setRequests[callId]
	c.setRequestMapLock.RUnlock()
	if !ok {
		log.Printf("[WARN] EndSet called for callId %s but there is no in progress set operation", callId)
		return fmt.Errorf("EndSet called for callId %s but there is no in progress set operation", callId)
	}

	log.Printf("[TRACE] EndSet (%s) table %s root key %s, pages: %d", callId, req.Table, req.resultKeyRoot, req.pageCount)

	// write the remaining buffered rows to the cache, then update the cache index
	if err := c.writePageToCache(ctx, req, true); err != nil {
		log.Printf("[WARN] QueryCache EndSet - result Set failed: %v", err)
		c.deletePages(req)
	} else if req.notCached {
		log.Printf("[INFO] QueryCache EndSet - a page did not fit in the cache, so the result is not cached (%s)", callId)
		c.deletePages(req)
	} else if err := c.updateIndex(ctx, callId, req); err != nil {
		log.Printf("[WARN] QueryCache EndSet - updating the cache index failed: %v (%s)", err, callId)
	}

	// remove entry from the map
	c.setRequestMapLock.Lock()
	delete(c.setRequests, callId)
	c.setRequestMapLock.Unlock()

	// clear the corresponding pending item - we have completed the transfer
	// (we need to do this even if the cache set fails)
	log.Printf("[INFO] calling pendingItemComplete (%s)", callId)
	c.pendingItemComplete(req.CacheRequest)

	// wait for all subscribers to complete, including the query that started the scan
	req.waitForSubscribers(ctx)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-req.ownSubscriber.doneChan:
	}
	log.Printf("[INFO] EndSet table %s %d rows (%s)", req.Table, req.rowCount, callId)
	return req.ownSubscriber.err
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
		pluralizeClient().Pluralize("subscriber", len(req.subscribers), false),
		req.CallId)

	req.state = requestError
	req.err = err
	req.requestLock.Unlock()

	log.Printf("[INFO] QueryCache AbortSet pendingItemComplete (%s)", req.CallId)
	// clear the corresponding pending item
	c.pendingItemComplete(req.CacheRequest)

	log.Printf("[INFO] QueryCache AbortSet - deleting %d pages from the cache (%s)", req.pageCount, req.CallId)
	c.deletePages(req)
	log.Printf("[INFO] QueryCache AbortSet done (%s)", req.CallId)
}

// deletePages removes the pages the scan has written from the cache.
// A page that a reader still has pinned is removed when the reader releases it.
func (c *QueryCache) deletePages(req *setRequest) {
	for _, pageKey := range req.pageKeys(0, int(req.pageCount)) {
		c.cache.delete(pageKey)
	}
}

// ClearForConnection removes all cache entries for the given connection
func (c *QueryCache) ClearForConnection(ctx context.Context, connectionName string) error {
	c.cache.invalidateTag(connectionName)
	return nil
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

// writePageToCache writes the buffered rows to the cache as the scan's next page.
// Each reader of the scan still needs the page, so it is pinned once for each of them.
// If the page does not fit in the cache, the readers still get it, but the result is not cached.
// If the page cannot be written at all, the scan fails, and so do its readers.
func (c *QueryCache) writePageToCache(ctx context.Context, req *setRequest, finalPage bool) error {
	// only the scan changes the page buffer, and the scan is here, so the rows do not change
	// while they are encoded outside the lock
	rows := req.getBufferedRows()
	pageBytes, err := proto.Marshal(&sdkproto.QueryResult{Rows: rows})

	// now lock the request
	req.requestLock.Lock()
	defer req.requestLock.Unlock()

	if err != nil {
		log.Printf("[WARN] writePageToCache failed to encode page %d: %v (%s)", req.pageCount, err, req.CallId)
		req.state = requestError
		req.err = err
		return err
	}

	// put the page in the cache before readers can see it, so a reader never misses it.
	// put connection name in tags
	pageKey := getPageKey(req.resultKeyRoot, int(req.pageCount))
	log.Printf("[TRACE] QueryCache writePageToCache: %d rows, page key %s, %d readers (%s)", len(rows), pageKey, len(req.subscribers), req.CallId)
	if err := c.cache.set(pageKey, pageBytes, []string{req.ConnectionName}, len(req.subscribers)); err != nil {
		log.Printf("[WARN] writePageToCache: %v - page key %s, so the result will not be cached (%s)", err, pageKey, req.CallId)
		req.notCached = true
	}

	// start a new buffer for the next page, and increment the page count
	// Do not reuse the old buffer: subscribers may still be streaming rows from it.
	req.pageBuffer = make([]*sdkproto.Row, rowBufferSize)
	req.bufferIndex = 0
	req.pageCount++

	// set completion state if this is last page
	if finalPage {
		req.state = requestComplete
	}
	return nil
}

func (c *QueryCache) getCachedIndexBucket(ctx context.Context, key string) (*IndexBucket, error) {
	var indexBucket = &sdkproto.IndexBucket{}
	if err := doGet(key, c.cache, indexBucket); err != nil {
		if IsCacheMiss(err) {
			c.Stats.Misses.Add(1)
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
		c.Stats.Misses.Add(1)
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
	str := c.sanitiseKey(fmt.Sprintf("%s_%s%s_%s_%d_%s",
		req.ConnectionName,
		req.Table,
		qualString,
		strings.Join(req.Columns, ","),
		req.Limit,
		req.sortOrderString()))
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
	// Load the schema map atomically to avoid race with updateConnectionConfigs
	schemaMapPtr := c.pluginSchemaMap.Load()
	if schemaMapPtr == nil {
		return res
	}
	schema, ok := (*schemaMapPtr)[connectionName]
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
	str = strings.ReplaceAll(str, "\n", "")
	str = strings.ReplaceAll(str, "\t", "")
	return str
}

// write index bucket back to cache
func (c *QueryCache) cacheSetIndexBucket(ctx context.Context, indexBucketKey string, indexBucket *IndexBucket, req *CacheRequest) error {
	log.Printf("[TRACE] cacheSetIndexBucket %s", indexBucketKey)

	// put connection name in tags
	tags := []string{req.ConnectionName}
	return doSet(indexBucketKey, indexBucket.AsProto(), c.cache, tags)
}

func doGet[T CacheData](key string, cache *pageStore, target T) error {
	// get the bytes from the cache
	bytes, ok := cache.get(key)
	if !ok {
		log.Printf("[TRACE] doGet cache miss ")
		return CacheMissError{}
	}

	// unmarshall into the correct type
	err := proto.Unmarshal(bytes, target)
	if err != nil {
		log.Printf("[WARN] error unmarshalling result: %s", err.Error())
		return err
	}

	return nil
}

// doSet stores value under key. Every entry lives for the cache's max TTL from when it is written.
func doSet[T CacheData](key string, value T, cache *pageStore, tags []string) error {
	bytes, err := proto.Marshal(value)
	if err != nil {
		log.Printf("[WARN] doSet - marshal failed: %v", err)
		return err
	}

	err = cache.set(key, bytes, tags, 0)
	if err != nil {
		log.Printf("[WARN] doSet cache.Set failed: %v", err)
	}

	return err
}

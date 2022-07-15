package query_cache

import (
	"context"
	"fmt"
	"github.com/turbot/steampipe-plugin-sdk/v3/error_helpers"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v3/telemetry"
)

// default ttl - increase this if any client has a larger ttl
const defaultTTL = 5 * time.Minute

type CacheMissError struct{}

func (CacheMissError) Error() string { return "cache miss" }

func IsCacheMiss(err error) bool {
	if err == nil {
		return false
	}

	return err.Error() == CacheMissError{}.Error()
}

type CacheStats struct {
	// keep count of hits and misses
	Hits   int
	Misses int
}

type QueryCache struct {
	Stats      *CacheStats
	pluginName string
	// map of connection name to plugin schema
	PluginSchemaMap map[string]*grpc.PluginSchema
	pendingData     map[string]*pendingIndexBucket
	pendingDataLock sync.Mutex
	ttlLock         sync.Mutex
	ttl             time.Duration
	cacheStream     proto.WrapperPlugin_EstablishCacheConnectionServer
	streamLock      sync.Mutex

	// map of callback functions for interested cache listeners, keyed by call id
	listeners    map[string]CacheCallback
	listenerLock sync.Mutex
}

type CacheCallback func(*proto.CacheResponse)

func NewQueryCache(pluginName string, cacheStream proto.WrapperPlugin_EstablishCacheConnectionServer, pluginSchemaMap map[string]*grpc.PluginSchema) (*QueryCache, error) {
	cache := &QueryCache{
		Stats:           &CacheStats{},
		pluginName:      pluginName,
		PluginSchemaMap: pluginSchemaMap,
		pendingData:     make(map[string]*pendingIndexBucket),
		ttl:             defaultTTL,
		cacheStream:     cacheStream,
		listeners:       make(map[string]CacheCallback),
	}

	go cache.streamListener()
	log.Printf("[INFO] query cache created")
	return cache, nil
}

// SetCacheStream sets the cache stream connection
// if we already have a stream, it;s possible the cache server has reconnected
// - call any pending callback functions with a suitable error
//func (c *QueryCache) SetCacheStream(cacheStream proto.WrapperPlugin_EstablishCacheConnectionServer) {
//	// if there is already a cache stream, call back any existing listener with an error response
//	if c.cacheStream != nil {
//		log.Printf("[WARN] QueryCache SetCacheStream called when there is already a cache stream - sending errors to any waiting callbacks")
//		resp := &proto.CacheResponse{
//			Error: "cache stream reconnected",
//		}
//		for _, cb := range c.listeners {
//			cb(resp)
//		}
//	}
//	// now store new stream
//	c.cacheStream = cacheStream
//}

func (c *QueryCache) Get(ctx context.Context, table string, qualMap map[string]*proto.Quals, columns []string, limit, clientTTLSeconds int64, rowCallback CacheCallback, callId, connectionName string) (err error) {
	ctx, span := telemetry.StartSpan(ctx, "QueryCache.Get (%s)", table)
	defer func() {
		//span.SetAttributes(attribute.Bool("cache-hit", cacheHit))
		span.End()
	}()

	// if the client TTL is greater than the cache TTL, update the cache value to match the client value
	c.setTtl(clientTTLSeconds)

	// get the index bucket for this table and quals
	// - this contains cache keys for all cache entries for specified table and quals
	indexBucketKey := c.buildIndexKey(connectionName, table)

	log.Printf("[TRACE] QueryCache Get - indexBucketKey %s, quals", indexBucketKey)

	// do we have a cached result?
	err = c.getCachedResult(indexBucketKey, table, qualMap, columns, limit, clientTTLSeconds, rowCallback, callId, connectionName)

	if IsCacheMiss(err) {
		log.Printf("[TRACE] getCachedResult returned CACHE MISS - checking for pending transfers")
		// there was no cached result - is there data fetch in progress?
		if pendingItem := c.getPendingResultItem(indexBucketKey, table, qualMap, columns, limit, connectionName); pendingItem != nil {
			log.Printf("[TRACE] found pending item - waiting for it")
			// so there is a pending result, wait for it
			return c.waitForPendingItem(ctx, pendingItem, indexBucketKey, table, qualMap, columns, limit, clientTTLSeconds, rowCallback, callId, connectionName)
		}
	}
	return err
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

func (c *QueryCache) StartSet(row *proto.Row, table string, qualMap map[string]*proto.Quals, columns []string, limit int64, callId, connectionName string) (resultKey string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
			log.Printf("[WARN] QueryCache Set suffered a panic: %s", err.Error())
		}
	}()

	log.Printf("[TRACE] QueryCache StartSet - connectionName: %s, table: %s, columns: %s, limit %d\n", connectionName, table, columns, limit)

	// write to the result cache
	resultKey = c.buildResultKey(table, qualMap, columns, limit, connectionName)

	// send CacheCommand_SET_RESULT_START command but do not register listeners for response
	err = c.cacheStreamSend(&proto.CacheRequest{
		Command: proto.CacheCommand_SET_RESULT_START,
		Key:     resultKey,
		Ttl:     int64(c.ttl.Seconds()),
		Result:  &proto.QueryResult{Rows: []*proto.Row{row}},
		CallId:  callId,
	})
	if err != nil {
		return "", err
	}

	return resultKey, nil
}

func (c *QueryCache) IterateSet(rows []*proto.Row, callId string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] QueryCache IterateSet suffered a panic: %s", helpers.ToError(r))
		}
	}()

	// send CacheCommand_SET_RESULT_ITERATE command but do not register listeners for response
	err := c.cacheStreamSend(&proto.CacheRequest{
		Command: proto.CacheCommand_SET_RESULT_ITERATE,
		Result:  &proto.QueryResult{Rows: rows},
		CallId:  callId,
	})
	if err != nil {
		log.Printf("[WARN] send failure for CacheCommand_SET_RESULT_ITERATE command: %s", err)
		// if there was an error, try to abort the set operation
		// - this will tell the cache server to clear any incomplete data for this set operation
		c.AbortSet(callId)
	}
}

func (c *QueryCache) EndSet(rows []*proto.Row, table string, qualMap map[string]*proto.Quals, columns []string, limit int64, callId, connectionName, resultKey string) {
	defer func() {
		if r := recover(); r != nil {

			log.Printf("[WARN] QueryCache EndSet suffered a panic: %v", helpers.ToError(r))
			// if there was an error, try to abort the set operation
			// - this will tell the cache server to clear any incomplete data for this set operation
			c.AbortSet(callId)
		}

		// clear the corresponding pending item - we have completed the transfer
		// (we need to do this even if the cache set fails)
		c.pendingItemComplete(table, qualMap, columns, limit, connectionName)
	}()

	log.Printf("[TRACE] QueryCache EndSet")

	// call EndSet command
	req := &proto.CacheRequest{
		Command: proto.CacheCommand_SET_RESULT_END,
		Result:  &proto.QueryResult{Rows: rows},
		CallId:  callId,
	}
	resp, err := c.streamCacheCommand(req)
	if resp.Error != "" && err == nil {
		err = fmt.Errorf(resp.Error)
	}
	if err != nil {
		log.Printf("[WARN] QueryCache EndSet failed %v, calling CacheCommand_SET_RESULT_ABORT", err)
		c.AbortSet(callId)
		return
	}

	log.Printf("[TRACE] QueryCache EndSet - CacheCommand_SET_RESULT_END command executed")

	// now write index bucket

	// now update the index
	// get the index bucket for this table and quals
	indexBucketKey := c.buildIndexKey(connectionName, table)
	indexBucket, err := c.cacheGetIndexBucket(indexBucketKey, callId)
	if err != nil {
		if IsCacheMiss(err) {
			log.Printf("[TRACE] cacheGetIndexBucket returned cache miss")
		} else {
			// if there is an error fetching the index bucket, log it and return
			// we do not want to risk overwriting an existing index bucketan
			log.Printf("[WARN] cacheGetIndexBucket failed: %v", err)
			return
		}
	}

	indexItem := NewIndexItem(columns, resultKey, limit, qualMap)
	if indexBucket == nil {
		// create new index bucket
		indexBucket = newIndexBucket()
	}
	indexBucket.Append(indexItem)
	log.Printf("[TRACE] Added index item to bucket, key %s", resultKey)

	if err := c.cacheSetIndexBucket(indexBucketKey, indexBucket, c.ttl, callId); err != nil {
		log.Printf("[WARN] cache Set failed for index bucket: %v", err)
	}
	log.Printf("[TRACE] QueryCache EndSet - IndexBucket written")
}

func (c *QueryCache) AbortSet(callId string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] QueryCache IterateSet suffered a panic: %s", helpers.ToError(r))
		}
	}()

	// send CacheCommand_SET_RESULT_ITERATE command but do not register listeners for response
	if err := c.cacheStreamSend(&proto.CacheRequest{
		Command: proto.CacheCommand_SET_RESULT_ABORT,
		CallId:  callId,
	}); err != nil {
		log.Printf("[WARN] send failure for CacheCommand_SET_RESULT_ABORT command: %s", err)
	}
}

// CancelPendingItem cancels a pending item - called when an execute call fails for any reason
func (c *QueryCache) CancelPendingItem(table string, qualMap map[string]*proto.Quals, columns []string, limit int64, connectionName string) {
	log.Printf("[TRACE] QueryCache CancelPendingItem table: %s", table)
	// clear the corresponding pending item
	c.pendingItemComplete(table, qualMap, columns, limit, connectionName)
}

// try to fetch cached data for the given cache request
func (c *QueryCache) getCachedResult(indexBucketKey, table string, qualMap map[string]*proto.Quals, columns []string, limit, ttlSeconds int64, rowCallback CacheCallback, callId, connectionName string) error {
	if callId == "" {
		return fmt.Errorf("getCachedResult called with no call id")
	}
	keyColumns := c.getKeyColumnsForTable(table, connectionName)

	log.Printf("[TRACE] QueryCache getCachedResult - index bucket key: %s ttlSeconds %d\n", indexBucketKey, ttlSeconds)
	indexBucket, err := c.cacheGetIndexBucket(indexBucketKey, callId)
	if err != nil {
		return err
	}

	// now check whether we have a cache entry that covers the required quals and columns - check the index
	indexItem := indexBucket.Get(qualMap, columns, limit, ttlSeconds, keyColumns)
	if indexItem == nil {
		limitString := "NONE"
		if limit != -1 {
			limitString = fmt.Sprintf("%d", limit)
		}
		c.Stats.Misses++
		log.Printf("[WARN] getCachedResult - no cached data covers columns %v, limit %s\n", columns, limitString)
		return new(CacheMissError)
	}

	// so we have a cache index, retrieve the item
	log.Printf("[TRACE] fetch result from cache, key: '%s'", indexItem.Key)

	// wrap the callback to deregister listener on completion
	wrappedRowCallback := func(resp *proto.CacheResponse) {
		if !resp.Success || resp.QueryResult == nil || len(resp.QueryResult.Rows) == 0 {
			log.Printf("[TRACE] wrapped row callback unregistering listener")
			c.unregisterCallback(callId)

		}
		// invoke real callback
		rowCallback(resp)
	}
	// register the cache stream listener
	c.registerCallback(callId, wrappedRowCallback)

	err = c.cacheStreamSend(&proto.CacheRequest{
		Command: proto.CacheCommand_GET_RESULT,
		Key:     indexItem.Key,
		CallId:  callId,
	})

	if err != nil {
		log.Printf("[WARN] cacheGetResult Send failed %v", err)
		// unregister listener
		c.unregisterCallback(callId)
		return err
	}

	// TODO update stats with cache hit
	//
	//if !cacheHit {
	//	c.Stats.Misses++
	//	log.Printf("[TRACE] getCachedResult - no item retrieved for cache key %s", indexItem.Key)
	//	return false, nil
	//}
	//
	//c.Stats.Hits++

	return nil
}

func (c *QueryCache) buildIndexKey(connectionName, table string) string {
	str := c.sanitiseKey(fmt.Sprintf("index__%s%s",
		connectionName,
		table))
	return str
}

func (c *QueryCache) buildResultKey(table string, qualMap map[string]*proto.Quals, columns []string, limit int64, connectionName string) string {
	str := c.sanitiseKey(fmt.Sprintf("%s%s%s%s%d",
		connectionName,
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
func (c *QueryCache) getKeyColumnsForTable(table string, connectionName string) map[string]*proto.KeyColumn {
	res := make(map[string]*proto.KeyColumn)
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

// send command to cache and wait for reply
func (c *QueryCache) streamCacheCommand(req *proto.CacheRequest) (*proto.CacheResponse, error) {
	log.Printf("[TRACE] streamCacheCommand %s", req.Command)

	responseChannel := make(chan *proto.CacheResponse)
	callback := func(response *proto.CacheResponse) {
		log.Printf("[TRACE] cacheSetIndexBucket callback success: %v", response.Success)
		responseChannel <- response
	}

	// register the cache stream listener
	c.registerCallback(req.CallId, callback)
	// ensure to deregister the cache stream listener
	defer c.unregisterCallback(req.CallId)

	if err := c.cacheStreamSend(req); err != nil {
		log.Printf("[WARN] streamCacheCommand Send failed: %v", err)
		return nil, err
	}

	log.Printf("[TRACE] await cache response for %s", req.Command)
	const cacheCommandTimeout = 10 * time.Second
	var response *proto.CacheResponse
	// now select result channel
	select {
	case response = <-responseChannel:
	case <-time.After(cacheCommandTimeout):
		return nil, fmt.Errorf("cache command '%s' timed out", req.Command)
	}

	log.Printf("[TRACE] streamCacheCommand received response for %s", req.Command)

	return response, nil
}

func (c *QueryCache) cacheSetIndexBucket(key string, indexBucket *IndexBucket, ttl time.Duration, callId string) error {
	log.Printf("[TRACE] cacheSetIndexBucket %s", key)

	req := &proto.CacheRequest{
		Command:     proto.CacheCommand_SET_INDEX,
		Key:         key,
		Ttl:         int64(ttl.Seconds()),
		IndexBucket: indexBucket.AsProto(),
		CallId:      callId,
	}

	resp, err := c.streamCacheCommand(req)
	if err != nil {
		return err
	}

	if resp.Error != "" {
		return fmt.Errorf(resp.Error)
	}

	return nil
}

func (c *QueryCache) cacheSetResult(key string, result *proto.QueryResult, ttl time.Duration, callId string) error {
	req := &proto.CacheRequest{
		Command: proto.CacheCommand_SET_RESULT_START,
		Key:     key,
		Ttl:     int64(ttl.Seconds()),
		Result:  result,
		CallId:  callId,
	}

	resp, err := c.streamCacheCommand(req)
	if err != nil {
		return err
	}

	if resp.Error != "" {
		return fmt.Errorf(resp.Error)
	}

	return nil
}

func (c *QueryCache) cacheGetIndexBucket(key, callId string) (*IndexBucket, error) {
	req := &proto.CacheRequest{
		Command: proto.CacheCommand_GET_INDEX,
		Key:     key,
		CallId:  callId,
	}

	resp, err := c.streamCacheCommand(req)
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf(resp.Error)
	}

	// was this a cache hit?
	if !resp.Success {
		log.Printf("[TRACE] cacheGetIndexBucket cache miss :(")
		c.Stats.Misses++
		return nil, new(CacheMissError)
	}
	// there should be an index in the respoonse
	if resp.IndexBucket == nil {
		log.Printf("[WARN] cacheGetIndexBucket cache hit but no index bucket was returned")
		return nil, fmt.Errorf("cacheGetIndexBucket cache hit but no index bucket was returned")
	}

	log.Printf("[TRACE] cacheGetIndexBucket cache hit ")
	var res = IndexBucketfromProto(resp.IndexBucket)
	return res, nil
}

// listen for messages on the cache stream and invoke the appropriate callback function
func (c *QueryCache) streamListener() {
	for {
		log.Printf("[TRACE] QueryCache streamListener waiting.... c.cacheStream %v", c.cacheStream)

		res, err := c.cacheStream.Recv()
		log.Printf("[TRACE] QueryCache streamListener received response (err %v)", err)
		if err != nil {
			c.logReceiveError(err)
			continue
		}
		callId := res.CallId
		if callId == "" {
			log.Printf("[WARN] cache response has no call id")
			continue
		}

		// is there a callback registered for this call id?
		callback, ok := c.getCallback(callId)
		if !ok {
			log.Printf("[WARN] no cache command listener registered for call id %s", callId)
			continue
		}
		log.Printf("[TRACE] invoking listener callback for call id %s", callId)

		// invoke the callback
		callback(res)
	}
}

func (c *QueryCache) logReceiveError(err error) {

	switch {
	case grpc.IsEOFError(err):
		log.Printf("[TRACE] streamListener received EOF")
	case error_helpers.IsContextCancelledError(err):
		// ignore
	default:
		log.Printf("[ERROR] error in streamListener: %v", err)
	}
}

// callback handling

func (c *QueryCache) registerCallback(callId string, callback CacheCallback) {
	c.listenerLock.Lock()
	defer c.listenerLock.Unlock()

	c.listeners[callId] = callback
}

func (c *QueryCache) unregisterCallback(callId string) {
	c.listenerLock.Lock()
	defer c.listenerLock.Unlock()

	delete(c.listeners, callId)
}

func (c *QueryCache) getCallback(callId string) (CacheCallback, bool) {
	c.listenerLock.Lock()
	defer c.listenerLock.Unlock()

	callback, ok := c.listeners[callId]
	return callback, ok
}

// send the request on the cache stream, locking the mutex to avoid clashes
func (c *QueryCache) cacheStreamSend(req *proto.CacheRequest) error {
	c.streamLock.Lock()
	defer c.streamLock.Unlock()
	return c.cacheStream.Send(req)
}

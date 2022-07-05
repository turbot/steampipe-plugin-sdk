package cache

import (
	"context"
	"fmt"
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
	_, ok := err.(CacheMissError)
	return ok
}

type CacheStats struct {
	// keep count of hits and misses
	Hits   int
	Misses int
}

type QueryCache struct {
	Stats           *CacheStats
	pluginName      string
	connectionName  string
	PluginSchema    map[string]*proto.TableSchema
	pendingData     map[string]*pendingIndexBucket
	pendingDataLock sync.Mutex
	ttlLock         sync.Mutex
	ttl             time.Duration
	cacheStream     proto.WrapperPlugin_EstablishCacheConnectionServer

	// map of callback functions for interested cache listeners, keyed by call id
	listeners    map[string]CacheCallback
	listenerLock sync.Mutex
}

type CacheCallback func(*proto.CacheResponse)

func NewQueryCache(pluginName, connectionName string, pluginSchema map[string]*proto.TableSchema, cacheStream proto.WrapperPlugin_EstablishCacheConnectionServer) (*QueryCache, error) {
	cache := &QueryCache{
		Stats:          &CacheStats{},
		pluginName:     pluginName,
		connectionName: connectionName,
		PluginSchema:   pluginSchema,
		pendingData:    make(map[string]*pendingIndexBucket),
		ttl:            defaultTTL,
		cacheStream:    cacheStream,
		listeners:      make(map[string]CacheCallback),
	}

	go cache.streamListener()
	log.Printf("[INFO] query cache created")
	return cache, nil
}

// TODO is this valid? do we need to clear the map of callbacks - or invoke all their error functions
// do we need to restart listener
func (c *QueryCache) SetCacheStream(cacheStream proto.WrapperPlugin_EstablishCacheConnectionServer) {
	c.cacheStream = cacheStream
}

func (c *QueryCache) Get(ctx context.Context, table string, qualMap map[string]*proto.Quals, columns []string, limit, clientTTLSeconds int64, rowCallback CacheCallback) (err error) {
	ctx, span := telemetry.StartSpan(ctx, "QueryCache.Get (%s)", table)
	defer func() {
		//span.SetAttributes(attribute.Bool("cache-hit", cacheHit))
		span.End()
	}()

	// generate call id
	callId := grpc.BuildCallId()

	// if the client TTL is greater than the cache TTL, update the cache value to match the client value
	c.setTtl(clientTTLSeconds)

	// get the index bucket for this table and quals
	// - this contains cache keys for all cache entries for specified table and quals
	indexBucketKey := c.buildIndexKey(c.connectionName, table)

	log.Printf("[TRACE] QueryCache Get - indexBucketKey %s, quals", indexBucketKey)

	// do we have a cached result?
	err = c.getCachedResult(indexBucketKey, table, qualMap, columns, limit, clientTTLSeconds, rowCallback, callId)
	log.Printf("[WARN] getCachedResult returned %v", err)
	if IsCacheMiss(err) {
		log.Printf("[WARN] CACHE MISS")
		// TODO make this work again
		// there was no cached result - is there data fetch in progress?
		//if pendingItem := c.getPendingResultItem(indexBucketKey, table, qualMap, columns, limit); pendingItem != nil {
		//	log.Printf("[TRACE] found pending item - waiting for it")
		//	// so there is a pending result, wait for it
		//	return c.waitForPendingItem(ctx, pendingItem, indexBucketKey, table, qualMap, columns, limit, clientTTLSeconds, rowCallback)
		//}
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

func (c *QueryCache) StartSet(row *proto.Row, table string, qualMap map[string]*proto.Quals, columns []string, limit int64) (callId string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
			log.Printf("[WARN] QueryCache Set suffered a panic: %s", err.Error())
		}

		// clear the corresponding pending item - we have completed the transfer
		// (we need to do this even if the cache set fails)
		c.pendingItemComplete(table, qualMap, columns, limit)
	}()

	// generate a callid for thie operation
	callId = grpc.BuildCallId()

	// TODO THINK ABOUT TTL
	// get ttl - read here in case the property is updated  between the 2 uses below
	ttl := c.ttl

	// ensure we include all returned columns in teh cache index
	columns = c.buildColumnsFromRow(row, columns)
	log.Printf("[TRACE] QueryCache StartSet - connectionName: %s, table: %s, columns: %s, limit %d\n", c.connectionName, table, columns, limit)

	// write to the result cache
	resultKey := c.buildResultKey(table, qualMap, columns, limit)

	// send CacheCommand_SET_RESULT_START command but do not register listeners for response
	err = c.cacheStream.Send(&proto.CacheRequest{
		Command: proto.CacheCommand_SET_RESULT_START,
		Key:     resultKey,
		Ttl:     int64(ttl.Seconds()),
		Result:  &proto.QueryResult{Rows: []*proto.Row{row}},
		CallId:  callId,
	})
	if err != nil {
		return "", err
	}

	return callId, nil
}

func (c *QueryCache) IterateSet(row *proto.Row, callId string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] QueryCache IterateSet suffered a panic: %s", helpers.ToError(r))
		}
	}()

	// send CacheCommand_SET_RESULT_ITERATE command but do not register listeners for response
	if err := c.cacheStream.Send(&proto.CacheRequest{
		Command: proto.CacheCommand_SET_RESULT_ITERATE,
		Result:  &proto.QueryResult{Rows: []*proto.Row{row}},
		CallId:  callId,
	}); err != nil {
		log.Printf("[WARN] send failure for CacheCommand_SET_RESULT_ITERATE command: %s", err)
		// TODO CANCEL the ongoing set operation - ABORT_SET
	}
}

func (c *QueryCache) EndSet(table string, qualMap map[string]*proto.Quals, columns []string, limit int64, callId string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] QueryCache EndSet suffered a panic: %s", helpers.ToError(r))
		}

		// clear the corresponding pending item - we have completed the transfer
		// (we need to do this even if the cache set fails)
		c.pendingItemComplete(table, qualMap, columns, limit)
	}()

	// call EndSet command
	req := &proto.CacheRequest{
		Command: proto.CacheCommand_SET_RESULT_END,
		CallId:  callId,
	}
	resp, err := c.streamCacheCommand(req)
	if err != nil {
		log.Printf("[WARN] cache EndSet: %s", err.Error())
	}
	if resp.Error != "" {
		log.Printf("[WARN] cache EndSet: %s", resp.Error)
	}

	// now write index bucket

	// TODO use resultkey from startset
	resultKey := c.buildResultKey(table, qualMap, columns, limit)

	// now update the index
	// get the index bucket for this table and quals
	indexBucketKey := c.buildIndexKey(c.connectionName, table)
	indexBucket, err := c.cacheGetIndexBucket(indexBucketKey, callId)
	if err != nil {
		if IsCacheMiss(err) {
			log.Printf("[WARN] cacheGetIndexBucket returned cache miss")
		} else {
			log.Printf("[WARN] cacheGetIndexBucket failed: %v", err)
		}
	}
	indexItem := NewIndexItem(columns, resultKey, limit, qualMap)
	if indexBucket == nil {
		// create new index bucket
		indexBucket = newIndexBucket()
	}
	indexBucket.Append(indexItem)

	if err := c.cacheSetIndexBucket(indexBucketKey, indexBucket, c.ttl, callId); err != nil {
		log.Printf("[WARN] cache Set failed for index bucket: %v", err)
	}
}

// inspect the result row to build a full list of columns
func (c *QueryCache) buildColumnsFromRow(row *proto.Row, columns []string) []string {
	for col := range row.Columns {
		if !helpers.StringSliceContains(columns, col) {
			columns = append(columns, col)
		}
	}
	sort.Strings(columns)
	return columns
}

// CancelPendingItem cancels a pending item - called when an execute call fails for any reason
func (c *QueryCache) CancelPendingItem(table string, qualMap map[string]*proto.Quals, columns []string, limit int64) {
	log.Printf("[TRACE] QueryCache CancelPendingItem table: %s", table)
	// clear the corresponding pending item
	c.pendingItemComplete(table, qualMap, columns, limit)
}

// try to fetch cached data for the given cache request
func (c *QueryCache) getCachedResult(indexBucketKey, table string, qualMap map[string]*proto.Quals, columns []string, limit, ttlSeconds int64, rowCallback CacheCallback, callId string) error {
	if callId == "" {
		return fmt.Errorf("getCachedResult called with no call id")
	}
	keyColumns := c.getKeyColumnsForTable(table)

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
		log.Printf("[TRACE] getCachedResult - no cached data covers columns %v, limit %s\n", columns, limitString)
		return new(CacheMissError)
	}

	// so we have a cache index, retrieve the item
	log.Printf("[WARN] fetch result from cache, key: '%s'", indexItem.Key)

	// wrap the callback to deregister listener on completion
	wrappedRowCallback := func(resp *proto.CacheResponse) {
		if !resp.Success || resp.QueryResult == nil || len(resp.QueryResult.Rows) == 0 {
			log.Printf("[WARN] wrapped row callback unregistering listener")
			// unregister listener
			delete(c.listeners, callId)

		}
		// invoke real callback
		rowCallback(resp)
	}
	// register the cache stream listener
	c.listeners[callId] = wrappedRowCallback

	err = c.cacheStream.Send(&proto.CacheRequest{
		Command: proto.CacheCommand_GET_RESULT,
		Key:     indexItem.Key,
		CallId:  callId,
	})

	if err != nil {
		log.Printf("[WARN] cacheGetResult Send failed %v", err)
		// unregister listener
		delete(c.listeners, callId)
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

// TODO move to cache server
//func (c *QueryCache) logMetrics() {
//	log.Printf("[TRACE] ------------------------------------ ")
//	log.Printf("[TRACE] Cache Metrics ")
//	log.Printf("[TRACE] ------------------------------------ ")
//	log.Printf("[TRACE] MaxCost: %d", c.cache.MaxCost())
//	log.Printf("[TRACE] KeysAdded: %d", c.cache.Metrics.KeysAdded())
//	log.Printf("[TRACE] CostAdded: %d", c.cache.Metrics.CostAdded())
//	log.Printf("[TRACE] KeysEvicted: %d", c.cache.Metrics.KeysEvicted())
//	log.Printf("[TRACE] CostEvicted: %d", c.cache.Metrics.CostEvicted())
//	log.Printf("[TRACE] ------------------------------------ ")
//}

func (c *QueryCache) sanitiseKey(str string) string {
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\t", "", -1)
	return str
}

// send command to cache and wait for reply
func (c *QueryCache) streamCacheCommand(req *proto.CacheRequest) (*proto.CacheResponse, error) {
	log.Printf("[WARN] streamCacheCommand %s", req.Command)

	responseChannel := make(chan *proto.CacheResponse)
	callback := func(response *proto.CacheResponse) {
		log.Printf("[WARN] cacheSetIndexBucket callback success %v", response.Success)
		responseChannel <- response
	}

	// register the cache stream listener
	c.listeners[req.CallId] = callback

	if err := c.cacheStream.Send(req); err != nil {
		log.Printf("[WARN] streamCacheCommand Send failed: %v", err)
		return nil, err
	}

	log.Printf("[WARN] await cache response")
	var response *proto.CacheResponse
	// now select result channel
	// TODO timeout
	select {
	case response = <-responseChannel:
	}

	// deregister the cache stream listener
	delete(c.listeners, req.CallId)

	log.Printf("[WARN] streamCacheCommand received response")

	return response, nil
}

func (c *QueryCache) cacheSetIndexBucket(key string, indexBucket *IndexBucket, ttl time.Duration, callId string) error {
	log.Printf("[WARN] cacheSetIndexBucket %s", key)

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

	//responseChannel := make(chan *proto.CacheResponse)
	//callback := func(response *proto.CacheResponse) {
	//	log.Printf("[WARN] cacheSetIndexBucket callback response %v", response)
	//	responseChannel <- response
	//}
	//
	//// register the cache stream listener
	//c.listeners[callId] = callback
	//
	//if err := c.cacheStream.Send(&proto.CacheRequest{
	//	Command:     proto.CacheCommand_SET_INDEX,
	//	Key:         key,
	//	Ttl:         int64(ttl.Seconds()),
	//	IndexBucket: indexBucket.AsProto(),
	//	CallId:      callId,
	//}); err != nil {
	//	log.Printf("[WARN] cacheSetIndexBucket Send failed: %v", err)
	//	return err
	//}
	//
	//log.Printf("[WARN] await cache response")
	//var setResponse *proto.CacheResponse
	//// now select result channel
	//// TODO timeout
	//select {
	//case setResponse = <-responseChannel:
	//}
	//
	//// deregister the cache stream listener
	//delete(c.listeners, callId)
	//
	//log.Printf("[WARN] setResponse: %v", setResponse)
	//
	//if setResponse.Error != "" {
	//	return fmt.Errorf(setResponse.Error)
	//}
	//return nil
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

	//responseChannel := make(chan *proto.CacheResponse)
	//callback := func(response *proto.CacheResponse) {
	//	log.Printf("[WARN] cacheSetResult callback response %v", response)
	//	responseChannel <- response
	//}
	//// register the cache stream listener
	//c.listeners[callId] = callback
	//
	//if err := c.cacheStream.Send(&proto.CacheRequest{
	//	Command: proto.CacheCommand_SET_RESULT_START,
	//	Key:     key,
	//	Ttl:     int64(ttl.Seconds()),
	//	Result:  result,
	//	CallId:  callId,
	//}); err != nil {
	//	return err
	//}
	//
	//log.Printf("[WARN] await cache response")
	//var setResponse *proto.CacheResponse
	//// now select result channel
	//// TODO timeout
	//select {
	//case setResponse = <-responseChannel:
	//}
	//
	//// deregister the cache stream listener
	//delete(c.listeners, callId)
	//
	//log.Printf("[WARN] setResponse: %v", setResponse)
	//
	//if setResponse.Error != "" {
	//	return fmt.Errorf(setResponse.Error)
	//}
	//return nil
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
		log.Printf("[WARN] cacheGetIndexBucket cache miss")
		c.Stats.Misses++
		return nil, new(CacheMissError)
	}
	// there should be an index in the respoonse
	if resp.IndexBucket == nil {
		log.Printf("[WARN] " +
			"cacheGetIndexBucket cache hit but no index bucket was returned")
		return nil, fmt.Errorf("cacheGetIndexBucket cache hit but no index bucket was returned")
	}

	log.Printf("[WARN] cacheGetIndexBucket cache hit ")
	var res = IndexBucketfromProto(resp.IndexBucket)
	return res, nil

	//
	//
	//
	//
	//log.Printf("[WARN] cacheGetIndexBucket for key '%s', callId '%s'", key, callId)
	//
	//responseChannel := make(chan *proto.CacheResponse)
	//callback := func(response *proto.CacheResponse) {
	//	log.Printf("[WARN] cacheGetIndexBucket callback response %v", response)
	//	responseChannel <- response
	//}
	//// register the cache stream listener
	//c.listeners[callId] = callback
	//
	//log.Printf("[WARN] send cache request")
	//err := c.cacheStream.Send(&proto.CacheRequest{
	//	Command: proto.CacheCommand_GET_INDEX,
	//	Key:     key,
	//	CallId:  callId,
	//})
	//if err != nil {
	//	log.Printf("[WARN] cacheGetIndexBucket Send failed: %v", err)
	//	return nil, err
	//}
	//
	//log.Printf("[WARN] await cache response")
	//var getResponse *proto.CacheResponse
	//// now select result channel
	//// TODO timeout
	//select {
	//case getResponse = <-responseChannel:
	//}
	//
	//// deregister the cache stream listener
	//delete(c.listeners, callId)
	//
	//log.Printf("[WARN] getResponse: %v", getResponse)
	//
	//if getResponse.Error != "" {
	//	return nil, fmt.Errorf(getResponse.Error)
	//}
	//// was this a cache hit?
	//if !getResponse.Success {
	//	log.Printf("[WARN] cacheGetIndexBucket cache miss")
	//	c.Stats.Misses++
	//	return nil, new(CacheMissError)
	//}
	//// there should be an index in the respoonse
	//if getResponse.IndexBucket == nil {
	//	log.Printf("[WARN] " +
	//		"cacheGetIndexBucket cache hit but no index bucket was returned")
	//	return nil, fmt.Errorf("cacheGetIndexBucket cache hit but no index bucket was returned")
	//}
	//
	//log.Printf("[WARN] cacheGetIndexBucket cache hit ")
	//var res = IndexBucketfromProto(getResponse.IndexBucket)
	//return res, nil
}

//func (c *QueryCache) cacheGetResult(key string, rowCallback CacheCallback, callId string) error {
//	log.Printf("[WARN] cacheGetResult %s", key)
//
//	// register the cache stream listener
//	c.listeners[callId] = rowCallback
//
//	err := c.cacheStream.Send(&proto.CacheRequest{
//		Command: proto.CacheCommand_GET_RESULT,
//		Key:     key,
//		CallId:  callId,
//	})
//
//	if err != nil {
//		log.Printf("[WARN] cacheGetResult Send failed %v", err)
//		return err
//	}
//
//	return nil
//
//	//// TODO TIMEOUT??
//	//// now wait for a response
//	//getResponse, err := c.cacheStream.Recv()
//	//if err != nil {
//	//	log.Printf("[WARN] cacheGetResult Recv failed %v", err)
//	//	return false, err
//	//}
//	//
//	//// was this a cache hit?
//	//if !getResponse.Success {
//	//	log.Printf("[WARN] cacheGetResult returned cache miss")
//	//	return false, nil
//	//}
//	//// there should be an index in the response
//	//if getResponse.QueryResult == nil {
//	//	log.Printf("[WARN] cacheGetResult cache hit but no result was returned")
//	//	return false, fmt.Errorf("cacheGetResult cache hit but no result was returned")
//	//}
//	//
//	//log.Printf("[WARN] cacheGetResult cache hit")
//	////res := getResponse.QueryResult
//	//return true, nil
//}

// listen for messages on the cache stream and invoke the appropriate callback function
func (c *QueryCache) streamListener() {
	for {
		log.Printf("[WARN] QueryCache streamListener waiting....")
		res, err := c.cacheStream.Recv()
		log.Printf("[WARN] QueryCache streamListener received response (err %v)", err)
		// TODO how to handle error?
		if err != nil {
			log.Printf("[WARN] error receiving cache stream data: %s", err.Error())
			continue
		}
		callId := res.CallId
		if callId == "" {
			log.Printf("[WARN] cache response has no call id")
			continue
		}

		// is there a callback registered for this call id?
		c.listenerLock.Lock()
		callback, ok := c.listeners[callId]
		c.listenerLock.Unlock()
		if !ok {
			log.Printf("[WARN] no cache command listener registered for call id %s", callId)
			continue
		}
		log.Printf("[WARN] invoking listener callback for call id %s", callId)
		// invoke the callback
		callback(res)
	}
}

// calculate the size of the cached data
//func (c *QueryCache) calcCost(table string, columns []string, result *proto.QueryResult) int {
//	// map of the actual sizes of each column type
//	costMap := map[proto.ColumnType]int{
//		proto.ColumnType_BOOL:      61,
//		proto.ColumnType_INT:       68,
//		proto.ColumnType_DOUBLE:    68,
//		proto.ColumnType_STRING:    76,
//		proto.ColumnType_LTREE:     76,
//		proto.ColumnType_JSON:      84,
//		proto.ColumnType_IPADDR:    85,
//		proto.ColumnType_CIDR:      88,
//		proto.ColumnType_INET:      96,
//		proto.ColumnType_DATETIME:  92,
//		proto.ColumnType_TIMESTAMP: 92,
//	}
//
//	tableSchema := c.PluginSchema[table]
//	columnsMap := tableSchema.GetColumnMap()
//
//	// build lists of dynamically sized columns
//	var jsonColumns []string
//	var ltreeColumns []string
//	var stringColumns []string
//
//	// result struct overhead is 52 bytes
//	baseRowCost := 52
//
//	for _, col := range columns {
//		colType := columnsMap[col].Type
//		// add base cost for a row of this column type
//		baseRowCost += costMap[colType]
//		// add in column name
//		baseRowCost += len(col)
//
//		//log.Printf("[WARN] col %s cost %d, key length %d = %d  (%d)", col, costMap[colType], len(col), costMap[colType]+len(col), baseRowCost)
//
//		if colType == proto.ColumnType_JSON {
//			jsonColumns = append(jsonColumns, col)
//		} else if colType == proto.ColumnType_LTREE {
//			ltreeColumns = append(ltreeColumns, col)
//		} else if colType == proto.ColumnType_STRING {
//			stringColumns = append(stringColumns, col)
//		}
//	}
//
//	cost := len(result.Rows) * baseRowCost
//
//	//log.Printf("[WARN] calcCost %d total columns, %d json columns %d string columns %d ltree columns", len(columns), len(jsonColumns), len(stringColumns), len(ltreeColumns))
//	//log.Printf("[WARN] base cost %d", cost)
//	if len(jsonColumns)+len(ltreeColumns)+len(stringColumns) > 0 {
//		for _, r := range result.Rows {
//			for _, c := range jsonColumns {
//				jsonResult := r.Columns[c].GetJsonValue()
//				cost += len(jsonResult)
//			}
//			for _, c := range ltreeColumns {
//				ltreeResult := r.Columns[c].GetLtreeValue()
//				cost += len(ltreeResult)
//			}
//			for _, c := range stringColumns {
//				stringResult := r.Columns[c].GetStringValue()
//				cost += len(stringResult)
//			}
//		}
//	}
//	log.Printf("[TRACE] calcCost for table '%s', %d rows, cost: %d", table, len(result.Rows), cost)
//	return cost
//}

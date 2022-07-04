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

type CacheCallback func(*proto.CacheResponse, error)

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

func (c *QueryCache) Get(ctx context.Context, table string, qualMap map[string]*proto.Quals, columns []string, limit, clientTTLSeconds int64, rowCallback CacheCallback) (cacheHit bool, err error) {

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

	// build a map containing only the quals which we use for building a cache key (i.e. key column quals)
	cacheQualMap := c.buildCacheQualMap(table, qualMap)

	// do we have a cached result?
	cacheHit, err = c.getCachedResult(indexBucketKey, table, cacheQualMap, columns, limit, clientTTLSeconds, rowCallback, callId)
	if err != nil {
		return false, err
	}
	if !cacheHit {
		// there was no cached result - is there data fetch in progress?
		if pendingItem := c.getPendingResultItem(indexBucketKey, table, cacheQualMap, columns, limit); pendingItem != nil {
			log.Printf("[TRACE] found pending item - waiting for it")
			// so there is a pending result, wait for it
			return c.waitForPendingItem(ctx, pendingItem, indexBucketKey, table, cacheQualMap, columns, limit, clientTTLSeconds, rowCallback)
		}
	}
	log.Printf("[INFO] CACHE MISS")
	// cache miss
	return false, nil
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

func (c *QueryCache) StartSet(table string, qualMap map[string]*proto.Quals, columns []string, limit int64) (callId string, res bool) {
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

	// generate a callid for thie operation
	callId = grpc.BuildCallId()

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

	if err := c.cacheSetResult(resultKey, result, ttl); err != nil {
		log.Printf("[WARN] cache Set failed: %v", err)
		return "", false
	}

	// now update the index
	// get the index bucket for this table and quals
	indexBucketKey := c.buildIndexKey(c.connectionName, table)
	indexBucket, err := c.cacheGetIndexBucket(indexBucketKey, callId)
	if err != nil {
		log.Printf("[WARN] cacheGetIndexBucket failed: %v", err)
	}
	indexItem := NewIndexItem(columns, resultKey, limit, cacheQualMap)
	if indexBucket == nil {
		// create new index bucket
		indexBucket = newIndexBucket()
	}
	indexBucket.Append(indexItem)

	if err := c.cacheSetIndexBucket(indexBucketKey, indexBucket, ttl); err != nil {
		log.Printf("[WARN] cache Set failed for index bucket: %v", err)
		return "", false
	}

	//// wait for value to pass through cache buffers
	//time.Sleep(10 * time.Millisecond)
	////c.logMetrics()

	return callId, true
}

func (c *QueryCache) IterateSet(row *proto.Row, callId string) (res bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] QueryCache Set suffered a panic: %s", helpers.ToError(r))
			res = false
		}

	}()

	//// wait for value to pass through cache buffers
	//time.Sleep(10 * time.Millisecond)
	////c.logMetrics()

	return true
}

func (c *QueryCache) EndSet(table string, qualMap map[string]*proto.Quals, columns []string, limit int64, callId string) (res bool) {
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

	// if any data was returned, extract the columns from the first row

	// TODO result key
	resultKey := ""
	// now update the index
	// get the index bucket for this table and quals
	indexBucketKey := c.buildIndexKey(c.connectionName, table)
	indexBucket, err := c.cacheGetIndexBucket(indexBucketKey, callId)
	if err != nil {
		log.Printf("[WARN] cacheGetIndexBucket failed: %v", err)
	}
	indexItem := NewIndexItem(columns, resultKey, limit, cacheQualMap)
	if indexBucket == nil {
		// create new index bucket
		indexBucket = newIndexBucket()
	}
	indexBucket.Append(indexItem)

	if err := c.cacheSetIndexBucket(indexBucketKey, indexBucket, c.ttl); err != nil {
		log.Printf("[WARN] cache Set failed for index bucket: %v", err)
		return false
	}

	//// wait for value to pass through cache buffers
	//time.Sleep(10 * time.Millisecond)
	////c.logMetrics()

	return true
}

// CancelPendingItem cancels a pending item - called when an execute call fails for any reason
func (c *QueryCache) CancelPendingItem(table string, qualMap map[string]*proto.Quals, columns []string, limit int64) {
	log.Printf("[TRACE] QueryCache CancelPendingItem table: %s", table)
	// clear the corresponding pending item
	c.pendingItemComplete(table, qualMap, columns, limit)
}

func (c *QueryCache) buildCacheQualMap(table string, qualMap map[string]*proto.Quals) map[string]*proto.Quals {
	keyColumns := c.getKeyColumnsForTable(table)

	cacheQualMap := make(map[string]*proto.Quals)
	for col, quals := range qualMap {
		log.Printf("[TRACE] buildCacheQualMap col %s, quals %+v", col, quals)
		// if this column is a key column, include in key
		if _, ok := keyColumns[col]; ok {
			log.Printf("[TRACE] including column %s", col)
			cacheQualMap[col] = quals
		} else {
			log.Printf("[TRACE] excluding column %s", col)
		}
	}
	return cacheQualMap
}

func (c *QueryCache) getCachedResult(indexBucketKey, table string, qualMap map[string]*proto.Quals, columns []string, limit, ttlSeconds int64, rowCallback CacheCallback, callId string) (bool, error) {
	keyColumns := c.getKeyColumnsForTable(table)

	log.Printf("[TRACE] QueryCache getCachedResult - index bucket key: %s ttlSeconds %d\n", indexBucketKey, ttlSeconds)
	indexBucket, err := c.cacheGetIndexBucket(indexBucketKey, callId)
	if err != nil {
		return false, err
	}
	if indexBucket == nil {
		c.Stats.Misses++
		log.Printf("[TRACE] getCachedResult - no index bucket")
		return false, nil
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
		return false, nil
	}

	// so we have a cache index, retrieve the item
	err = c.cacheGetResult(indexItem.Key, rowCallback, callId)
	if err != nil {
		return false, err
	}

	//
	//if !cacheHit {
	//	c.Stats.Misses++
	//	log.Printf("[TRACE] getCachedResult - no item retrieved for cache key %s", indexItem.Key)
	//	return false, nil
	//}
	//
	//c.Stats.Hits++

	return true, nil
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
	} else {
		log.Printf("[WARN] getKeyColumnsForTable found NO SCHEMA FOR %s", table)
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

func (c *QueryCache) cacheSetIndexBucket(key string, indexBucket *IndexBucket, ttl time.Duration) error {
	log.Printf("[WARN] cacheSetIndexBucket %s", key)

	if err := c.cacheStream.Send(&proto.CacheRequest{
		Command:     proto.CacheCommand_SET_INDEX,
		Key:         key,
		Ttl:         int64(ttl.Seconds()),
		IndexBucket: indexBucket.AsProto(),
	}); err != nil {
		log.Printf("[WARN] cacheSetIndexBucket Send failed: %v", err)
		return err
	}

	return c.getSetResponse()
}

func (c *QueryCache) cacheSetResult(key string, result *proto.QueryResult, ttl time.Duration) error {
	if err := c.cacheStream.Send(&proto.CacheRequest{
		Command: proto.CacheCommand_SET_RESULT_START,
		Key:     key,
		Ttl:     int64(ttl.Seconds()),
		Result:  result,
	}); err != nil {
		return err
	}
	return c.getSetResponse()
}

func (c *QueryCache) getSetResponse() error {
	// now wait for a response
	res, err := c.cacheStream.Recv()
	if err != nil {
		log.Printf("[WARN] CacheStream Recv failed: %v", err)
		return err
	}
	if !res.Success {
		log.Printf("[WARN] cache set failed")
		return fmt.Errorf("cache set failed")
	}
	return nil
}

func (c *QueryCache) cacheGetIndexBucket(key, callId string) (*IndexBucket, error) {
	log.Printf("[WARN] cacheGetIndexBucket %s", key)

	responseChannel := make(chan *proto.CacheResponse)
	errorChannel := make(chan error)
	callback := func(response *proto.CacheResponse, err error) {

		if err != nil {
			log.Printf("[WARN] cacheGetIndexBucket callback error %s", err.Error())
			errorChannel <- err
		} else {
			log.Printf("[WARN] cacheGetIndexBucket callback response %v", response)
			responseChannel <- response
		}
	}
	// register the cache stream listener
	c.listeners[callId] = callback

	err := c.cacheStream.Send(&proto.CacheRequest{
		Command: proto.CacheCommand_GET_INDEX,
		Key:     key,
		CallId:  callId,
	})
	if err != nil {
		log.Printf("[WARN] cacheGetIndexBucket Send failed: %v", err)
		return nil, err
	}

	var getResponse *proto.CacheResponse
	// now select error and resilt channel
	select {
	case err := <-errorChannel:
		log.Printf("[WARN] cacheGetIndexBucket Recv failed: %v", err)
		return nil, err
	case getResponse = <-responseChannel:
	}

	// deregister the cach stream listener
	delete(c.listeners, callId)

	// was this a cache hit?
	if !getResponse.Success {
		log.Printf("[WARN] cacheGetIndexBucket cache miss")
		return nil, nil
	}
	// there should be an index in the respoonse
	if getResponse.IndexBucket == nil {
		log.Printf("[WARN] " +
			"cacheGetIndexBucket cache hit but no index bucket was returned")
		return nil, fmt.Errorf("cacheGetIndexBucket cache hit but no index bucket was returned")
	}

	log.Printf("[WARN] cacheGetIndexBucket cache hit ")
	var res = IndexBucketfromProto(getResponse.IndexBucket)
	return res, nil
}

func (c *QueryCache) cacheGetResult(key string, rowCallback CacheCallback, callId string) error {
	log.Printf("[WARN] cacheGetResult %s", key)

	// register the cache stream listener
	c.listeners[callId] = rowCallback

	err := c.cacheStream.Send(&proto.CacheRequest{
		Command: proto.CacheCommand_GET_RESULT,
		Key:     key,
		CallId:  callId,
	})

	if err != nil {
		log.Printf("[WARN] cacheGetResult Send failed %v", err)
		return err
	}

	return nil

	//// TODO TIMEOUT??
	//// now wait for a response
	//getResponse, err := c.cacheStream.Recv()
	//if err != nil {
	//	log.Printf("[WARN] cacheGetResult Recv failed %v", err)
	//	return false, err
	//}
	//
	//// was this a cache hit?
	//if !getResponse.Success {
	//	log.Printf("[WARN] cacheGetResult returned cache miss")
	//	return false, nil
	//}
	//// there should be an index in the response
	//if getResponse.QueryResult == nil {
	//	log.Printf("[WARN] cacheGetResult cache hit but no result was returned")
	//	return false, fmt.Errorf("cacheGetResult cache hit but no result was returned")
	//}
	//
	//log.Printf("[WARN] cacheGetResult cache hit")
	////res := getResponse.QueryResult
	//return true, nil
}

// listen for messages on the cache stream and invoke the appropriate callback function
func (c *QueryCache) streamListener() {
	for {
		res, err := c.cacheStream.Recv()
		// TODO how to handle error?
		if err != nil {
			log.Printf("[WARN] error receiving cache stream data: %s", err.Error())
			continue
		}
		callId := res.CallId
		// is there a callback registered for this call id?
		c.listenerLock.Lock()
		callback, ok := c.listeners[callId]
		if !ok {
			log.Printf("[WARN] no cache command listener registered for call id %s", callId)
			continue
		}
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

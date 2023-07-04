package query_cache

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
)

func (c *QueryCache) getPendingResultItem(indexBucketKey string, req *CacheRequest) *pendingIndexItem {
	log.Printf("[TRACE] getPendingResultItem indexBucketKey %s, columns %v, limit %d", indexBucketKey, req.Columns, req.Limit)

	// do we have a pending items which satisfy the qual, limit and column constraints
	pendingItems, _ := c.getPendingItemSatisfyingRequest(indexBucketKey, req)

	// if there was no pending result - we assume the calling code will fetch the data and add it to the cache
	// so add a pending result
	if len(pendingItems) == 0 {
		log.Printf("[TRACE] no pending index item - add pending result, indexBucketKey %s", indexBucketKey)
		// return nil (i.e. cache miss)
		return nil
	}

	// to get here we must have a non-empty list of pending items
	log.Printf("[TRACE] getPendingResultItem returning %v", pendingItems[0])
	// return pending item
	return pendingItems[0]
}

// this must be called inside a lock
func (c *QueryCache) getPendingItemSatisfyingRequest(indexBucketKey string, req *CacheRequest) ([]*pendingIndexItem, *pendingIndexBucket) {
	keyColumns := c.getKeyColumnsForTable(req.Table, req.ConnectionName)

	// is there a pending index bucket for this query
	if pendingIndexBucket, ok := c.pendingData[indexBucketKey]; ok {
		log.Printf("[TRACE] got pending index bucket, checking for pending item which satisfies columns and limit, indexBucketKey %s, columns %v, limit %d, quals %s (%s)",
			indexBucketKey, req.Columns, req.Limit, grpc.QualMapToLogLine(req.QualMap), req.CallId)
		// now check whether there is a pending item in this bucket that covers the required columns and limit
		return pendingIndexBucket.GetItemsSatisfyingRequest(req, keyColumns), pendingIndexBucket

	}
	return nil, nil
}

// this must be called inside a lock
func (c *QueryCache) getPendingItemResolvedByRequest(indexBucketKey string, req *CacheRequest) (*pendingIndexItem, *pendingIndexBucket) {
	c.pendingDataLock.RLock()
	defer func() {
		c.pendingDataLock.RUnlock()
	}()

	// is there a pending index bucket for this query
	if pendingIndexBucket, ok := c.pendingData[indexBucketKey]; ok {
		for _, item := range pendingIndexBucket.Items {
			if item.callId == req.CallId {
				return item, pendingIndexBucket
			}
		}
	}
	return nil, nil
}

func (c *QueryCache) addPendingResult(ctx context.Context, indexBucketKey string, req *CacheRequest, streamRowFunc func(row *proto.Row)) {
	// NOTE: this must be calling inside  c.pendingDataLock.Lock()

	// call startSet to add a setRequest to the setRequest map and subscribe to the data
	setRequest := c.startSet(ctx, req, streamRowFunc)

	log.Printf("[TRACE] addPendingResult (%s) indexBucketKey %s, columns %v, limit %d", req.CallId, indexBucketKey, req.Columns, req.Limit)

	// do we have a pending bucket?
	pendingIndexBucket, ok := c.pendingData[indexBucketKey]
	if !ok {
		log.Printf("[TRACE] no index bucket found - creating one")
		pendingIndexBucket = newPendingIndexBucket()
	}
	// use the root result key to key the pending item map
	resultKeyRoot := req.resultKeyRoot

	// create pending index item
	item := NewPendingIndexItem(setRequest)
	pendingIndexBucket.Items[resultKeyRoot] = item

	// now write back to pending data map
	c.pendingData[indexBucketKey] = pendingIndexBucket

	log.Printf("[TRACE] addPendingResult added pending index item to bucket, (%s) indexBucketKey %s, resultKeyRoot %s, pending item : %p", req.CallId, indexBucketKey, resultKeyRoot, item)

	c.logPending(req)
}

// unlock pending result items from the map
func (c *QueryCache) pendingItemComplete(req *CacheRequest) {
	indexBucketKey := c.buildIndexKey(req.ConnectionName, req.Table)

	log.Printf("[TRACE] pendingItemComplete (%s) indexBucketKey %s, columns %v, limit %d", req.CallId, indexBucketKey, req.Columns, req.Limit)
	defer log.Printf("[TRACE] pendingItemComplete done (%s)", req.CallId)

	// do we have a pending items
	completedPendingItem, pendingIndexBucket := c.getPendingItemResolvedByRequest(indexBucketKey, req)

	if completedPendingItem == nil {
		log.Printf("[TRACE] pendingItemComplete - no pending item found (%s)", req.CallId)
		return
	}
	log.Printf("[TRACE] got completedPendingItem, (%s)", req.CallId)

	// acquire a Write lock for remainder of function
	c.pendingDataLock.Lock()
	// ensure we release lock
	defer c.pendingDataLock.Unlock()

	// if there is an error, ONLY remove the pending item corresponding to this request
	// remove pending item from the parent pendingIndexBucket (BEFORE updating the index item cache key)
	delete(pendingIndexBucket.Items, completedPendingItem.item.Key)

	// NOTE set the page count for the pending item to the actual page count, which we now know
	completedPendingItem.item.PageCount = req.pageCount
	// NOTE set the key for the pending item to be the root key of the completed request
	// this is necessary as this is the cache key which was actually used to insert the data
	completedPendingItem.item.Key = req.resultKeyRoot

	log.Printf("[TRACE] found completed pending item (%s) %p, key %s - removing from map as it is complete", req.CallId, completedPendingItem, completedPendingItem.item.Key)
	log.Printf("[TRACE] deleted from pending, (%s) len %d", req.CallId, len(pendingIndexBucket.Items))

	if len(pendingIndexBucket.Items) == 0 {
		log.Printf("[TRACE] pending bucket now empty - deleting key %s", indexBucketKey)
		delete(c.pendingData, indexBucketKey)
	}
}

func (c *QueryCache) logPending(req *CacheRequest) {
	log.Printf("[TRACE] **** pending items (%s)****", req.CallId)
	for bucketKey, pendingBucket := range c.pendingData {
		log.Printf("[TRACE] key: %s: %s", bucketKey, pendingBucket.String())
	}
	log.Printf("[TRACE] ********")
}

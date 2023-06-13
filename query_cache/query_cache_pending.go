package query_cache

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
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
func (c *QueryCache) getPendingItemResolvedByRequest(indexBucketKey string, req *CacheRequest) ([]*pendingIndexItem, *pendingIndexBucket) {
	keyColumns := c.getKeyColumnsForTable(req.Table, req.ConnectionName)

	// is there a pending index bucket for this query
	if pendingIndexBucket, ok := c.pendingData[indexBucketKey]; ok {
		log.Printf("[TRACE] got pending index bucket, checking for pending item satisfied by columns and limit, indexBucketKey %s, columns %v, limit %d, quals %s (%s)",
			indexBucketKey, req.Columns, req.Limit, grpc.QualMapToLogLine(req.QualMap), req.CallId)
		// now check whether there is a pending item in this bucket that covers the required columns and limit
		return pendingIndexBucket.GetItemsSatisfiedByRequest(req, keyColumns), pendingIndexBucket

	}
	return nil, nil
}

//// this must be called inside a lock
//func (c *QueryCache) getPendingResultItem(indexBucketKey string, req *CacheRequest) *pendingIndexItem {
//	log.Printf("[TRACE] getPendingResultItem indexBucketKey %s, columns %v, limit %d", indexBucketKey, req.Columns, req.Limit)
//
//	keyColumns := c.getKeyColumnsForTable(req.Table, req.ConnectionName)
//
//	// is there a pending index bucket for this query
//	if pendingIndexBucket, ok := c.pendingData[indexBucketKey]; ok {
//		log.Printf("[TRACE] got pending index bucket, checking for pending item satisfied by columns and limit, indexBucketKey %s, columns %v, limit %d, quals %s (%s)",
//			indexBucketKey, req.Columns, req.Limit, grpc.QualMapToLogLine(req.QualMap), req.CallId)
//		// now check whether there is a pending item in this bucket that covers the required columns and limit
//		items := pendingIndexBucket.GetItemsSatisfiedByRequest(req, keyColumns)
//		if len(items) > 0 {
//			return items[0]
//		}
//	}
//
//	log.Printf("[TRACE] no pending index item for, indexBucketKey %s", indexBucketKey)
//
//	return nil
//}

func (c *QueryCache) addPendingResult(ctx context.Context, indexBucketKey string, req *CacheRequest) {
	// NOTE: this must be calling inside  c.pendingDataLock.Lock()

	// call start set to add the request to the setRequest map
	setRequest := c.startSet(ctx, req)
	// this must be called within a pendingDataLock Write Lock
	log.Printf("[WARN] addPendingResult (%s) indexBucketKey %s, columns %v, limit %d", req.CallId, indexBucketKey, req.Columns, req.Limit)

	// do we have a pending bucket
	pendingIndexBucket, ok := c.pendingData[indexBucketKey]
	if !ok {
		log.Printf("[WARN] no index bucket found - creating one")
		pendingIndexBucket = newPendingIndexBucket()
	}
	// use the root result key to key the pending item map
	resultKeyRoot := req.resultKeyRoot

	// this pending item _may_ already exist - if we have previously fetched the same data (perhaps the ttl expired)
	// create a new one anyway to replace that one
	// NOTE: when creating a pending item the lock wait group is incremented automatically
	item := NewPendingIndexItem(setRequest)
	pendingIndexBucket.Items[resultKeyRoot] = item

	// now write back to pending data map
	c.pendingData[indexBucketKey] = pendingIndexBucket

	log.Printf("[WARN] addPendingResult added pending index item to bucket, (%s) indexBucketKey %s, resultKeyRoot %s, pending item : %p", req.CallId, indexBucketKey, resultKeyRoot, item)

	c.logPending(req)
}

// unlock pending result items from the map
func (c *QueryCache) pendingItemComplete(req *CacheRequest, err error) {
	indexBucketKey := c.buildIndexKey(req.ConnectionName, req.Table)

	log.Printf("[TRACE] pendingItemComplete (%s) indexBucketKey %s, columns %v, limit %d", req.CallId, indexBucketKey, req.Columns, req.Limit)
	defer log.Printf("[TRACE] pendingItemComplete done (%s)", req.CallId)

	// acquire a Read lock to pendingData map
	c.pendingDataLock.RLock()
	// do we have a pending items
	// the may be more than one pending item which is satisfied by this request - clear them all
	completedPendingItems, _ := c.getPendingItemResolvedByRequest(indexBucketKey, req)
	// release read lock
	c.pendingDataLock.RUnlock()

	if len(completedPendingItems) > 0 {
		log.Printf("[TRACE] got completedPendingItems, (%s) len %d", req.CallId, len(completedPendingItems))

		// acquire a Write lock
		c.pendingDataLock.Lock()
		// ensure we release lock
		defer c.pendingDataLock.Unlock()

		// check again for completed items (in case anyone else grabbed a Write lock before us)
		completedPendingItems, pendingIndexBucket := c.getPendingItemResolvedByRequest(indexBucketKey, req)
		for _, pendingItem := range completedPendingItems {
			// remove pending item from the parent pendingIndexBucket (BEFORE updating the index item cache key)
			delete(pendingIndexBucket.Items, pendingItem.item.Key)

			// NOTE set the page count for the pending item to the actual page count, which we now know
			pendingItem.item.PageCount = req.pageCount
			// NOTE set the key for the pending item to be the root key of the completed request
			// this is necessary as this is the cache key which was actually used to insert the data
			pendingItem.item.Key = req.resultKeyRoot

			log.Printf("[TRACE] found completed pending item (%s) %p, key %s - removing from map as it is complete", req.CallId, pendingItem, pendingItem.item.Key)
			log.Printf("[TRACE] deleted from pending, (%s) len %d", req.CallId, len(pendingIndexBucket.Items))
		}
		if len(pendingIndexBucket.Items) == 0 {
			log.Printf("[TRACE] pending bucket now empty - deleting key %s", indexBucketKey)
			delete(c.pendingData, indexBucketKey)
		}
	}
}

func (c *QueryCache) logPending(req *CacheRequest) {
	log.Printf("[TRACE] **** pending items (%s)****", req.CallId)
	for bucketKey, pendingBucket := range c.pendingData {
		log.Printf("[TRACE] key: %s: %s", bucketKey, pendingBucket.String())
	}
	log.Printf("[TRACE] ********")
}

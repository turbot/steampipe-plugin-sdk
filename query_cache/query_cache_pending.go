package query_cache

import (
	"context"
	"log"
	"time"

	sdkproto "github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v4/telemetry"
)

const pendingQueryTimeout = 40 * time.Second

func (c *QueryCache) getPendingResultItem(indexBucketKey string, req *CacheRequest) *pendingIndexItem {
	log.Printf("[WARN] getPendingResultItem indexBucketKey %s, columns %v, limit %d", indexBucketKey, req.Columns, req.Limit)
	var pendingItem *pendingIndexItem

	// lock access to pending data map
	c.pendingDataLock.Lock()
	defer c.pendingDataLock.Unlock()

	// do we have a pending bucket
	pendingIndexBucket, ok := c.pendingData[indexBucketKey]
	if ok {
		log.Printf("[WARN] got pending index bucket, checking for pending item which satisfies columns and limi, indexBucketKey %s, columnd %v, limid %d", indexBucketKey, req.Columns, req.Limit)
		// is there a pending index bucket for this query
		// now check whether there is a pending item in this bucket that covers the required columns and limit
		pendingItem = pendingIndexBucket.GetItemWhichSatisfiesColumnsAndLimit(req.Columns, req.Limit)
	}

	// if there was no pending result  -  we assume the calling code will fetch the data and add it to the cache
	// so add a pending result
	if pendingItem == nil {
		log.Printf("[WARN] no pending index item - add pending result, indexBucketKey %s", indexBucketKey)
		// add a pending result so anyone else asking for this data will wait the fetch to complete
		c.addPendingResult(indexBucketKey, req)
	}

	log.Printf("[TRACE] getPendingResultItem returning %v", pendingItem)
	// return pending item, which may be nil, i.e. a cache miss
	return pendingItem
}

func (c *QueryCache) waitForPendingItem(ctx context.Context, pendingItem *pendingIndexItem, indexBucketKey string, req *CacheRequest, streamRowFunc func(row *sdkproto.Row)) (err error) {
	ctx, span := telemetry.StartSpan(ctx, c.pluginName, "QueryCache.waitForPendingItem (%s)", req.Table)
	defer span.End()

	transferCompleteChan := make(chan bool, 1)
	go func() {
		log.Printf("[WARN] waitForPendingItem %p indexBucketKey: %s, item key %s", pendingItem, indexBucketKey, pendingItem.item.Key)
		pendingItem.Wait()
		log.Printf("[WARN] pending item COMPLETE %p indexBucketKey: %s, item key %s", pendingItem, indexBucketKey, pendingItem.item.Key)
		close(transferCompleteChan)
	}()

	select {
	case <-ctx.Done():
		log.Printf("[WARN] waitForPendingItem aborting as context cancelled")
		// TODO is this right? How is it handled upstream
		err = ctx.Err()

	case <-time.After(pendingQueryTimeout):
		log.Printf("[WARN] waitForPendingItem timed out waiting for pending transfer %p, callId %s, indexBucketKey: %s, item key %s", pendingItem, req.CallId, indexBucketKey, pendingItem.item.Key)

		// remove the pending result from the map
		// lock access to pending results map
		c.pendingDataLock.Lock()
		if c.pendingData[indexBucketKey] != nil {
			// c.pendingData[indexBucketKey] may be nil
			// remove the pending item - it has timed out
			c.pendingData[indexBucketKey].delete(pendingItem)
		} else {
			// not expected
			log.Printf("[ERROR] no index bucket found for timed out pending item, indexBucketKey: %s, item key %s", indexBucketKey, pendingItem.item.Key)
		}
		// add a new pending item, within the lock
		c.addPendingResult(indexBucketKey, req)
		c.pendingDataLock.Unlock()
		log.Printf("[TRACE] added new pending item, returning cache miss")
		// return cache miss error to force a fetch
		err = CacheMissError{}

	case <-transferCompleteChan:
		log.Printf("[TRACE] waitForPendingItem transfer complete - trying cache again, indexBucketKey: %s, item key %s", indexBucketKey, pendingItem.item.Key)

		// now try to read from the cache again
		var err error

		_, err = c.getCachedQueryResult(ctx, indexBucketKey, req, streamRowFunc)
		if err != nil {
			log.Printf("[WARN] waitForPendingItem - index item %s, transferCompleteChan was signalled but getCachedResult returned error: %v", pendingItem.item.Key, err)
			// if the data is still not in the cache, create a pending item
			if IsCacheMiss(err) {
				log.Printf("[WARN] waitForPendingItem item still not in the cache - add pending item, indexBucketKey: %s, item key %s", indexBucketKey, pendingItem.item.Key)
				// lock access to pending results map
				c.pendingDataLock.Lock()
				// add a new pending item, within the lock
				c.addPendingResult(indexBucketKey, req)
				c.pendingDataLock.Unlock()
			}
		} else {
			log.Printf("[WARN] waitForPendingItem retrieved from cache, indexBucketKey: %s, item key %s", indexBucketKey, pendingItem.item.Key)
		}
	}
	return err
}

func (c *QueryCache) addPendingResult(indexBucketKey string, req *CacheRequest) {
	// this must be called within a pendingDataLock
	log.Printf("[WARN] addPendingResult indexBucketKey %s, columns %v, limit %d", indexBucketKey, req.Columns, req.Limit)

	// do we have a pending bucket
	pendingIndexBucket, ok := c.pendingData[indexBucketKey]
	if !ok {
		log.Printf("[TRACE] no index bucket found - creating one")
		pendingIndexBucket = newPendingIndexBucket()
	}
	// build a result key
	resultKey := c.buildResultKey(req)

	// this pending item _may_ already exist - if we have previously fetched the same data (perhaps the ttl expired)
	// create a new one anyway to replace that one
	// NOTE: when creating a pending item the lock wait group is incremented automatically
	item := NewPendingIndexItem(req)
	pendingIndexBucket.Items[resultKey] = item

	// now write back to pending data map
	c.pendingData[indexBucketKey] = pendingIndexBucket

	log.Printf("[WARN] addPendingResult added pending index item to bucket, indexBucketKey %s, resultKey %s, pending item : %p", indexBucketKey, resultKey, item)

}

// unlock pending result items from the map
func (c *QueryCache) pendingItemComplete(req *CacheRequest) {
	indexBucketKey := c.buildIndexKey(req.ConnectionName, req.Table)

	log.Printf("[TRACE] pendingItemComplete indexBucketKey %s, columns %v, limit %d", indexBucketKey, req.Columns, req.Limit)
	defer log.Printf("[TRACE] pendingItemComplete done")

	// lock access to pending data map
	c.pendingDataLock.Lock()
	defer c.pendingDataLock.Unlock()

	// do we have a pending bucket
	if pendingIndexBucket, ok := c.pendingData[indexBucketKey]; ok {
		log.Printf("[TRACE] got pending index bucket, len %d", len(pendingIndexBucket.Items))
		// the may be more than one pending item which is satisfied by this request - clear them all
		completedPendingItems := pendingIndexBucket.GetItemsSatisfiedByColumns(req.Columns, req.Limit)
		for _, pendingItem := range completedPendingItems {
			log.Printf("[TRACE] found completed pending item %p, key %s - removing from map as it is complete", pendingItem, pendingItem.item.Key)
			// unlock the item
			pendingItem.Unlock()
			// remove it from the map
			delete(pendingIndexBucket.Items, pendingItem.item.Key)
			log.Printf("[TRACE] deleted from pending, len %d", len(pendingIndexBucket.Items))
		}
		if len(pendingIndexBucket.Items) == 0 {
			log.Printf("[TRACE] pending bucket now empty - deleting key %s", indexBucketKey)
			delete(c.pendingData, indexBucketKey)
		}
	}
}

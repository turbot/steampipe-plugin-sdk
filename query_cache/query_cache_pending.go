package query_cache

import (
	"context"
	"log"
	"time"

	sdkproto "github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v3/telemetry"
)

const pendingQueryTimeout = 10 * time.Second

func (c *QueryCache) getPendingResultItem(indexBucketKey string, req *CacheRequest) *pendingIndexItem {
	log.Printf("[TRACE] getPendingResultItem indexBucketKey %s, columns %v, limit %d", indexBucketKey, req.Columns, req.Limit)
	var pendingItem *pendingIndexItem

	// lock access to pending data map
	c.pendingDataLock.Lock()
	defer c.pendingDataLock.Unlock()

	// do we have a pending bucket
	pendingIndexBucket, ok := c.pendingData[indexBucketKey]
	if ok {
		log.Printf("[TRACE] got pending index bucket, checking for pending item")
		// is there a pending index bucket for this query
		// now check whether there is a pending item in this bucket that covers the required columns and limit
		pendingItem = pendingIndexBucket.GetItemWhichSatisfiesColumnsAndLimit(req.Columns, req.Limit)
	}

	// if there was no pending result  -  we assume the calling code will fetch the data and add it to the cache
	// so add a pending result
	if pendingItem == nil {
		log.Printf("[TRACE] no pending index item - add pending result")
		// add a pending result so anyone else asking for this data will wait the fetch to complete
		c.addPendingResult(indexBucketKey, req)
	}

	log.Printf("[TRACE] getPendingResultItem returning %v", pendingItem)
	// return pending item, which may be nil, i.e. a cache miss
	return pendingItem
}

func (c *QueryCache) waitForPendingItem(ctx context.Context, pendingItem *pendingIndexItem, indexBucketKey string, req *CacheRequest) (result *sdkproto.QueryResult, err error) {
	ctx, span := telemetry.StartSpan(ctx, c.pluginName, "QueryCache.waitForPendingItem (%s)", req.Table)
	defer span.End()

	log.Printf("[TRACE] waitForPendingItem indexBucketKey: %s", indexBucketKey)

	transferCompleteChan := make(chan bool, 1)
	go func() {
		pendingItem.Wait()
		close(transferCompleteChan)
	}()

	select {
	case <-ctx.Done():
		log.Printf("[WARN] waitForPendingItem aborting as context cancelled")
		// TODO is this right? How is it handled upstream
		err = ctx.Err()

	case <-time.After(pendingQueryTimeout):
		log.Printf("[WARN] waitForPendingItem timed out waiting for pending transfer, indexBucketKey: %s", indexBucketKey)

		// remove the pending result from the map
		// lock access to pending results map
		c.pendingDataLock.Lock()
		// remove the pending item - it has timed out
		c.pendingData[indexBucketKey].delete(pendingItem)
		// add a new pending item, within the lock
		c.addPendingResult(indexBucketKey, req)
		c.pendingDataLock.Unlock()
		log.Printf("[TRACE] added new pending item, returning cache miss")
		// return cache miss error to force a fetch
		err = CacheMissError{}

	case <-transferCompleteChan:
		log.Printf("[TRACE] waitForPendingItem transfer complete - trying cache again, indexBucketKey: %s", indexBucketKey)

		// now try to read from the cache again
		var err error

		result, err = c.getCachedQueryResult(ctx, indexBucketKey, req)
		if err != nil {
			log.Printf("[WARN] waitForPendingItem - getCachedResult returned error: %v", err)
			// if the data is still not in the cache, create a pending item
			if IsCacheMiss(err) {
				log.Printf("[TRACE] waitForPendingItem item still not in the cache - add pending item, indexBucketKey: %s", indexBucketKey)
				// lock access to pending results map
				c.pendingDataLock.Lock()
				// add a new pending item, within the lock
				c.addPendingResult(indexBucketKey, req)
				c.pendingDataLock.Unlock()
			}
		} else {
			log.Printf("[TRACE] waitForPendingItem retrieved from cache, indexBucketKey: %s", indexBucketKey)
		}
	}
	return result, err
}

func (c *QueryCache) addPendingResult(indexBucketKey string, req *CacheRequest) {
	// this must be called within a pendingDataLock
	log.Printf("[TRACE] addPendingResult indexBucketKey %s, columns %v, limit %d", indexBucketKey, req.Columns, req.Limit)

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
	pendingIndexBucket.Items[resultKey] = NewPendingIndexItem(req.Columns, resultKey, req.Limit)

	// now write back to pending data map
	c.pendingData[indexBucketKey] = pendingIndexBucket
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
		pendingItems := pendingIndexBucket.GetItemsSatisfiedByColumns(req.Columns, req.Limit)
		for _, pendingItem := range pendingItems {
			log.Printf("[TRACE] got pending item %s - removing from map as it is complete", pendingItem.item.Key)
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

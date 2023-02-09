package query_cache

import (
	"context"
	"log"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v4/error_helpers"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v4/telemetry"
)

const pendingQueryTimeout = 20 * time.Second

func (c *QueryCache) getPendingResultItem(indexBucketKey string, req *CacheRequest) *pendingIndexItem {
	log.Printf("[TRACE] getPendingResultItem indexBucketKey %s, columns %v, limit %d", indexBucketKey, req.Columns, req.Limit)
	//c.logPending(req)
	var pendingItem *pendingIndexItem

	// lock access to pending data map
	c.pendingDataLock.Lock()
	defer c.pendingDataLock.Unlock()

	// do we have a pending bucket
	pendingIndexBucket, ok := c.pendingData[indexBucketKey]
	if ok {
		log.Printf("[TRACE] got pending index bucket, checking for pending item which satisfies columns and limit, indexBucketKey %s, columnd %v, limid %d", indexBucketKey, req.Columns, req.Limit)
		// is there a pending index bucket for this query
		// now check whether there is a pending item in this bucket that covers the required columns and limit
		pendingItem = pendingIndexBucket.GetItemWhichSatisfiesColumnsAndLimit(req.Columns, req.Limit)
	}

	// if there was no pending result  -  we assume the calling code will fetch the data and add it to the cache
	// so add a pending result
	if pendingItem == nil {
		log.Printf("[TRACE] no pending index item - add pending result, indexBucketKey %s", indexBucketKey)
		// add a pending result so anyone else asking for this data will wait the fetch to complete
		c.addPendingResult(indexBucketKey, req)
	}

	log.Printf("[TRACE] getPendingResultItem returning %v", pendingItem)
	// return pending item, which may be nil, i.e. a cache miss
	return pendingItem
}

func (c *QueryCache) waitForPendingItem(ctx context.Context, pendingItem *pendingIndexItem, indexBucketKey string, req *CacheRequest, streamRowFunc func(row *sdkproto.Row)) (err error) {
	ctx, span := telemetry.StartSpan(ctx, c.pluginName, "QueryCache.waitForPendingItem (%s)", req.Table)
	//c.pendingDataLock.Lock()
	//c.logPending(req)
	//c.pendingDataLock.Unlock()

	defer span.End()

	transferCompleteChan := make(chan bool, 1)
	errChan := make(chan error, 1)
	go func() {
		log.Printf("[TRACE] waitForPendingItem (%s) %p indexBucketKey: %s, item key %s", req.CallId, pendingItem, indexBucketKey, pendingItem.item.Key)
		// if pendingItem.Wait() returns an error it means the query we are waiting for failed - we should fail as well
		err := pendingItem.Wait()
		log.Printf("[TRACE] pendingItem.Wait() returned, error: %v", err)
		if err != nil {
			if !error_helpers.IsContextCancelledError(err) {
				log.Printf("[WARN] wrapping error %v in a QueryError and returning", err)
				// wrap the error in a query error to the calling code realizes this was not just a cache error
				err = error_helpers.NewQueryError(err)
			}
			errChan <- err
			return
		}

		log.Printf("[TRACE] pending item COMPLETE (%s) %p indexBucketKey: %s, item key %s", req.CallId, pendingItem, indexBucketKey, pendingItem.item.Key)
		close(transferCompleteChan)
	}()

	select {
	case <-ctx.Done():
		log.Printf("[TRACE] waitForPendingItem aborting as context cancelled")
		err = ctx.Err()

	case <-time.After(pendingQueryTimeout):
		log.Printf("[WARN] waitForPendingItem timed out waiting for pending transfer (%s) %p indexBucketKey: %s, item key %s", req.CallId, pendingItem, indexBucketKey, pendingItem.item.Key)

		// remove the pending result from the map
		// lock access to pending results map
		c.pendingDataLock.Lock()
		//c.logPending(req)

		// if the pending bucket still exists, delete the pending item
		//( it may not exists  if the pending item jhhas actually finished and we just missed the event)
		if pendingBucket, ok := c.pendingData[indexBucketKey]; ok {
			// c.pendingData[indexBucketKey] may be nil
			// remove the pending item - it has timed out
			pendingBucket.delete(pendingItem)
		}
		// add a new pending item, within the lock
		c.addPendingResult(indexBucketKey, req)
		c.pendingDataLock.Unlock()
		log.Printf("[TRACE] added new pending item, returning cache miss (%s)", req.CallId)
		// return cache miss error to force a fetch
		err = CacheMissError{}

	case <-transferCompleteChan:
		log.Printf("[TRACE] waitForPendingItem transfer complete - trying cache again, (%s) pending item %p index item %p indexBucketKey: %s, item key %s", req.CallId, pendingItem, pendingItem.item, indexBucketKey, pendingItem.item.Key)

		// now try to read from the cache again
		// NOTE: use same error variable, so we can return it
		err = c.getCachedQueryResultFromIndexItem(ctx, pendingItem.item, streamRowFunc)
		if err != nil {
			log.Printf("[WARN] waitForPendingItem (%s) - pending item %p, key %s, transferCompleteChan was signalled but getCachedResult returned error: %v", req.CallId, pendingItem, pendingItem.item.Key, err)
			// if the data is still not in the cache, create a pending item
			if IsCacheMiss(err) {
				log.Printf("[WARN] waitForPendingItem item still not in the cache - add pending item, (%s) indexBucketKey: %s, item key %s", req.CallId, indexBucketKey, pendingItem.item.Key)
				// lock access to pending results map
				c.pendingDataLock.Lock()
				// add a new pending item, within the lock
				c.addPendingResult(indexBucketKey, req)
				c.pendingDataLock.Unlock()
			}
		} else {
			log.Printf("[TRACE] waitForPendingItem retrieved from cache, (%s) indexBucketKey: %s, item key %s", req.CallId, indexBucketKey, pendingItem.item.Key)
		}
	case err = <-errChan:
		if !error_helpers.IsContextCancelledError(err) {
			log.Printf("[WARN] waitForPendingItem returned error %s", err.Error())
		}
		// fall through
	}
	return err
}

func (c *QueryCache) addPendingResult(indexBucketKey string, req *CacheRequest) {
	// this must be called within a pendingDataLock
	log.Printf("[TRACE] addPendingResult (%s) indexBucketKey %s, columns %v, limit %d", req.CallId, indexBucketKey, req.Columns, req.Limit)

	// do we have a pending bucket
	pendingIndexBucket, ok := c.pendingData[indexBucketKey]
	if !ok {
		log.Printf("[TRACE] no index bucket found - creating one")
		pendingIndexBucket = newPendingIndexBucket()
	}
	// use the root result key to key the pending item map
	resultKeyRoot := req.resultKeyRoot

	// this pending item _may_ already exist - if we have previously fetched the same data (perhaps the ttl expired)
	// create a new one anyway to replace that one
	// NOTE: when creating a pending item the lock wait group is incremented automatically
	item := NewPendingIndexItem(req)
	pendingIndexBucket.Items[resultKeyRoot] = item

	// now write back to pending data map
	c.pendingData[indexBucketKey] = pendingIndexBucket

	log.Printf("[TRACE] addPendingResult added pending index item to bucket, (%s) indexBucketKey %s, resultKeyRoot %s, pending item : %p", req.CallId, indexBucketKey, resultKeyRoot, item)

	c.logPending(req)
}

// unlock pending result items from the map
func (c *QueryCache) pendingItemComplete(req *CacheRequest, err error) {
	indexBucketKey := c.buildIndexKey(req.ConnectionName, req.Table)

	log.Printf("[TRACE] pendingItemComplete (%s) indexBucketKey %s, columns %v, limit %d", req.CallId, indexBucketKey, req.Columns, req.Limit)
	defer log.Printf("[TRACE] pendingItemComplete done (%s)", req.CallId)

	// lock access to pending data map
	c.pendingDataLock.Lock()
	defer c.pendingDataLock.Unlock()

	// do we have a pending bucket
	if pendingIndexBucket, ok := c.pendingData[indexBucketKey]; ok {
		log.Printf("[TRACE] got pending index bucket, (%s) len %d", req.CallId, len(pendingIndexBucket.Items))
		// the may be more than one pending item which is satisfied by this request - clear them all
		completedPendingItems := pendingIndexBucket.GetItemsSatisfiedByColumns(req.Columns, req.Limit)
		for _, pendingItem := range completedPendingItems {
			// remove pending item from the parent pendingIndexBucket (BEFORE updating the index item cache key)
			delete(pendingIndexBucket.Items, pendingItem.item.Key)

			// NOTE set the page count for the pending item to the actual page count, which we now know
			pendingItem.item.PageCount = req.pageCount
			// NOTE: set the key for the pending item to be the root key of the completed request
			// this is necessary as this is the cache key which was actually used to insert the data
			pendingItem.item.Key = req.resultKeyRoot

			log.Printf("[TRACE] found completed pending item (%s) %p, key %s - removing from map as it is complete", req.CallId, pendingItem, pendingItem.item.Key)
			// unlock the item passing err (which may be nil)
			pendingItem.Unlock(err)
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

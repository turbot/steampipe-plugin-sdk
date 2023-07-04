package query_cache

import (
	"context"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/error_helpers"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"golang.org/x/sync/semaphore"
	"log"
	"sync"
)

type cacheResultSubscriber struct {
	indexItem     *IndexItem
	req           *CacheRequest
	streamRowFunc func(row *sdkproto.Row)
	queryCache    *QueryCache
}

func newCacheResultSubscriber(c *QueryCache, indexItem *IndexItem, req *CacheRequest, streamRowFunc func(row *sdkproto.Row)) *cacheResultSubscriber {
	return &cacheResultSubscriber{
		indexItem:     indexItem,
		req:           req,
		streamRowFunc: streamRowFunc,
		queryCache:    c,
	}

}

func (s *cacheResultSubscriber) waitUntilDone(ctx context.Context) error {
	// so we have a cache index, retrieve the item
	log.Printf("[INFO] got an index item - try to retrieve rows from cache (%s)", s.req.CallId)

	cacheHit := true
	var errors []error
	errorChan := make(chan (error), s.indexItem.PageCount)
	var wg sync.WaitGroup
	const maxReadThreads = 5
	var maxReadSem = semaphore.NewWeighted(maxReadThreads)

	// define streaming function
	streamRows := func(cacheResult *sdkproto.QueryResult) {
		for _, r := range cacheResult.Rows {
			// check for context cancellation
			if error_helpers.IsContextCancelledError(ctx.Err()) {
				log.Printf("[INFO] getCachedQueryResult context cancelled - returning (%s)", s.req.CallId)
				return
			}
			s.streamRowFunc(r)
		}
	}
	// ok so we have an index item - we now stream
	// ensure the first page exists (evictions start with oldest item so if first page exists, they all exist)
	pageIdx := 0
	pageKey := getPageKey(s.indexItem.Key, pageIdx)
	var cacheResult = &sdkproto.QueryResult{}
	if err := doGet[*sdkproto.QueryResult](ctx, pageKey, s.queryCache.cache, cacheResult); err != nil {
		return err
	}

	// ok it's there, stream rows
	streamRows(cacheResult)
	// update page index
	pageIdx++

	// now fetch the rest (if any), in parallel maxReadThreads at a time
	for ; pageIdx < int(s.indexItem.PageCount); pageIdx++ {
		maxReadSem.Acquire(ctx, 1)
		wg.Add(1)
		// construct the page key, _using the index item key as the root_
		p := getPageKey(s.indexItem.Key, pageIdx)

		go func(pageKey string) {
			defer wg.Done()
			defer maxReadSem.Release(1)

			log.Printf("[TRACE] fetching key: %s", pageKey)
			var cacheResult = &sdkproto.QueryResult{}
			if err := doGet[*sdkproto.QueryResult](ctx, pageKey, s.queryCache.cache, cacheResult); err != nil {
				if IsCacheMiss(err) {
					// This is not expected
					log.Printf("[WARN] getCachedQueryResult - no item retrieved for cache key %s (%s)", pageKey, s.req.CallId)
				} else {
					log.Printf("[WARN] cacheGetResult Get failed %v (%s)", err, s.req.CallId)
				}
				errorChan <- err
				return
			}

			log.Printf("[TRACE] got result: %d rows", len(cacheResult.Rows))

			streamRows(cacheResult)
		}(p)
	}
	doneChan := make(chan bool)
	go func() {
		wg.Wait()
		close(doneChan)
	}()

	for {
		select {
		case err := <-errorChan:
			log.Printf("[WARN] cacheResultSubscriber waitUntilDone received error: %s (%s)", err.Error(), s.req.CallId)
			if IsCacheMiss(err) {
				cacheHit = false
			} else {
				errors = append(errors, err)
			}
		case <-doneChan:
			// any real errors return them
			if len(errors) > 0 {
				return helpers.CombineErrors(errors...)
			}
			if cacheHit {
				// this was a hit - return
				s.queryCache.Stats.Hits++
				return nil
			} else {
				s.queryCache.Stats.Misses++
				return CacheMissError{}
			}
		}
	}
}

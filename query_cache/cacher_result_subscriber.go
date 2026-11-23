package query_cache

import (
	"context"
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v6/error_helpers"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
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

// waitUntilDone streams the cached result. It streams every row, or returns a cache miss
// having streamed none: a cache miss after some rows would make the plugin fetch the
// data again and stream those rows twice.
func (s *cacheResultSubscriber) waitUntilDone(ctx context.Context) error {
	// so we have a cache index, retrieve the item
	log.Printf("[INFO] got an index item - try to retrieve rows from cache (%s)", s.req.CallId)

	cache := s.queryCache.cache
	// construct the page keys, _using the index item key as the root_
	pageKeys := make([]string, s.indexItem.PageCount)
	for pageIdx := range pageKeys {
		pageKeys[pageIdx] = getPageKey(s.indexItem.Key, pageIdx)
	}
	// pin every page before streaming any rows, so the cache keeps them until we have read them
	if !cache.pin(pageKeys) {
		log.Printf("[INFO] getCachedQueryResult - the cache no longer has every page of %s (%s)", s.indexItem.Key, s.req.CallId)
		s.queryCache.Stats.Misses.Add(1)
		return CacheMissError{}
	}

	errorChan := make(chan error, len(pageKeys))
	var wg sync.WaitGroup
	const maxReadThreads = 5
	var maxReadSem = semaphore.NewWeighted(maxReadThreads)

	// streamPage streams the rows of one page, then releases its pin
	streamPage := func(pageKey string) error {
		defer cache.unpin(pageKey)

		log.Printf("[TRACE] fetching key: %s", pageKey)
		var cacheResult = &sdkproto.QueryResult{}
		if err := doGet[*sdkproto.QueryResult](pageKey, cache, cacheResult); err != nil {
			// the page is pinned, so it should be there. Do not return a cache miss: rows may have been streamed
			log.Printf("[WARN] cacheGetResult Get failed %v (%s)", err, s.req.CallId)
			return fmt.Errorf("reading cached page %s: %s", pageKey, err.Error())
		}
		log.Printf("[TRACE] got result: %d rows", len(cacheResult.Rows))

		for _, r := range cacheResult.Rows {
			// check for context cancellation
			if error_helpers.IsContextCancelledError(ctx.Err()) {
				log.Printf("[INFO] getCachedQueryResult context cancelled - returning (%s)", s.req.CallId)
				return nil
			}
			s.streamRowFunc(r)
		}
		return nil
	}

	// stream the first page, then fetch the rest (if any), in parallel maxReadThreads at a time
	if err := streamPage(pageKeys[0]); err != nil {
		cache.unpin(pageKeys[1:]...)
		return err
	}
	pageIdx := 1
	for ; pageIdx < len(pageKeys); pageIdx++ {
		if err := maxReadSem.Acquire(ctx, 1); err != nil {
			// context was cancelled before we acquired a slot - do not spawn a
			// goroutine that would release a slot it never held. This page (and
			// every later one, since a cancelled context stays cancelled) is
			// silently dropped from the result with no error surfaced - the same
			// as streamPage already does when it detects cancellation mid-page,
			// so this does not introduce a new failure mode, just extends the
			// existing one to pages that had not started yet
			break
		}
		wg.Add(1)
		go func(pageKey string) {
			defer wg.Done()
			defer maxReadSem.Release(1)
			if err := streamPage(pageKey); err != nil {
				errorChan <- err
			}
		}(pageKeys[pageIdx])
	}
	// release the pins on the pages we did not read
	cache.unpin(pageKeys[pageIdx:]...)

	wg.Wait()
	close(errorChan)
	var errors []error
	for err := range errorChan {
		log.Printf("[WARN] cacheResultSubscriber waitUntilDone received error: %s (%s)", err.Error(), s.req.CallId)
		errors = append(errors, err)
	}
	if len(errors) > 0 {
		return helpers.CombineErrors(errors...)
	}
	// this was a hit - return
	s.queryCache.Stats.Hits.Add(1)
	return nil
}

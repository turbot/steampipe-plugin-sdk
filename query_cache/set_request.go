package query_cache

import (
	"context"
	"fmt"
	"github.com/gertd/go-pluralize"
	"github.com/sethvargo/go-retry"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
	"sync"
	"time"
)

type setRequestState int64

const (
	requestInProgress setRequestState = iota
	requestComplete
	requestError
)

type setRequest struct {
	*CacheRequest
	// other cache requests who are subscribing to this data
	subscribers map[*setRequestSubscriber]struct{}
	requestLock sync.RWMutex
	// flag used by subscribers when getting rows to stream
	state      setRequestState
	pageBuffer []*sdkproto.Row
	// index within the page buffer
	bufferIndex int
	err         error
	cache       *QueryCache
}

func newSetRequest(req *CacheRequest, cache *QueryCache) *setRequest {
	return &setRequest{
		CacheRequest: req,
		subscribers:  make(map[*setRequestSubscriber]struct{}),
		pageBuffer:   make([]*sdkproto.Row, rowBufferSize),
		cache:        cache,
		state:        requestInProgress,
	}
}

func (req *setRequest) subscribe(subscriber *setRequestSubscriber) {
	// note: requestLock must be locked when this is called
	req.subscribers[subscriber] = struct{}{}
}

func (req *setRequest) unsubscribe(subscriber *setRequestSubscriber) {
	// note: requestLock must be locked when this is called
	delete(req.subscribers, subscriber)
}

func (req *setRequest) getBufferedRows() []*sdkproto.Row {
	if req.bufferIndex == 0 {
		return nil
	}
	return req.pageBuffer[:req.bufferIndex]
}

// get result key for the most recent page of the request
func (req *setRequest) getPageResultKey() string {
	return getPageKey(req.resultKeyRoot, int(req.pageCount-1))
}

func (req *setRequest) getPrevPageResultKeys() []string {
	var res []string
	for i := 0; i < int(req.pageCount); i++ {
		res = append(res, getPageKey(req.resultKeyRoot, int(req.pageCount-1)))
	}
	return res
}

// return all rows available after the given row count
func (req *setRequest) getRowsSince(ctx context.Context, rowsAlreadyStreamed int) ([]*sdkproto.Row, error) {
	/*

		[0,1,2,3,4]
		req.rowCount = 5
		req.bufferIndex = 1
		req.pageCount = 0

		CASE 1:	rowsAlreadyStreamed = 0
		startPage = 0    -> rowsAlreadyStreamed / bufferSize
		startOffset = 0  -> rowsAlreadyStreamed % bufferSize

		CASE 2:	rowsAlreadyStreamed = 5
		startPage = 1    -> i.e. all rows already streamedm as startOffset > pakeCOunt
		startOffset = 0 -> rowsAlreadyStreamed % bufferSize



		[0,1,2,3,4] [0,1,2,3,4] [0,1,2,x,x]

		req.rowCount = 13
		req.bufferIndex = 3
		req.pageCount = 2


		CASE 1:	rowsAlreadyStreamed = 0

		startPage = 0    -> rowsAlreadyStreamed / bufferSize
		startOffset = 0  -> rowsAlreadyStreamed % bufferSize

		CASE 2:	rowsAlreadyStreamed = 3

		startPage = 0
		startOffset = 3  -> rowsAlreadyStreamed % bufferSize

		CASE 3:	rowsAlreadyStreamed = 5

		startPage = 1
		startOffset = 0  -> rowsAlreadyStreamed % bufferSize

		CASE 4:	rowsAlreadyStreamed = 8

		startPage = 1
		startOffset = 3  -> rowsAlreadyStreamed % bufferSize

		CASE 5:	rowsAlreadyStreamed = 10

		startPage = (2) -> i.e. no cache page as this is not yet cached
		startOffset = 0  -> rowsAlreadyStreamed % bufferSize


		CASE 5:	rowsAlreadyStreamed = 12

		startPage = (2) -> i.e. no cache page as this is not yet cached
		startOffset = 2  -> rowsAlreadyStreamed % bufferSize

	*/

	startPage := rowsAlreadyStreamed / rowBufferSize
	startOffset := rowsAlreadyStreamed % rowBufferSize

	log.Printf("[TRACE] setRequest getRowsSince rowsAlreadyStreamed: %d, req.pageCount: %d, req.bufferIndex: %d, startPage: %d startOffset: %d, ",
		rowsAlreadyStreamed,
		req.pageCount,
		req.bufferIndex,
		startPage,
		startOffset)

	// if startPage > pageCount this must mean that the buffer is full, but we have already written it all
	// so do nothing
	if startPage > int(req.pageCount) {
		return nil, nil
	}
	// if start page is the current page, this means we do not need any data written to the cache
	// just return rows from page buffer
	if startPage == int(req.pageCount) {
		log.Printf("[TRACE] setRequest getRowsSince returning %d from buffer", req.bufferIndex-startOffset)
		bufferedRows := req.getBufferedRows()
		// apply the start offset
		return bufferedRows[startOffset:], nil
	}

	// so we must load some data from the cache

	// build result rows
	var res = make([]*sdkproto.Row, 0, req.rowCount-rowsAlreadyStreamed)

	// load previously cache pages`
	for pageIdx := startPage; pageIdx < int(req.pageCount); pageIdx++ {
		// sometimes the cached page is not available to read immediately - retry if needed
		cachedResult, cacheErr := req.readPageFromCacheWithRetries(ctx, pageIdx)
		if cacheErr != nil {
			return nil, cacheErr
		}

		// copy cached rows into res - taking into account start offset if this is the first page
		idx := 0
		if pageIdx == startPage {
			idx = startOffset
		}
		res = append(res, cachedResult.Rows[idx:]...)
	}

	// now add any rows from the page buffer
	res = append(res, req.getBufferedRows()...)

	log.Printf("[TRACE] setRequest getRowsSince returning %d", len(res))
	return res, nil
}

func (req *setRequest) readPageFromCacheWithRetries(ctx context.Context, pageIdx int) (*sdkproto.QueryResult, error) {
	pageKey := getPageKey(req.resultKeyRoot, pageIdx)
	log.Printf("[INFO] readPageFromCacheWithRetries reading page %d key %s", pageIdx, pageKey)

	var cachedResult = &sdkproto.QueryResult{}
	var maxRetries uint64 = 10
	retryBackoff := retry.WithMaxRetries(
		maxRetries,
		retry.NewExponential(10*time.Millisecond),
	)

	retries := 0

	cacheErr := retry.Do(ctx, retryBackoff, func(ctx context.Context) error {
		err := doGet[*sdkproto.QueryResult](ctx, pageKey, req.cache.cache, cachedResult)
		if err != nil {

			if IsCacheMiss(err) {
				retries++

				log.Printf("[INFO] readPageFromCacheWithRetries got a cache miss - retrying (#%d) (%s)", retries, req.CallId)
				err = retry.RetryableError(err)
			} else {
				log.Printf("[WARN] readPageFromCacheWithRetries got error: %s", err.Error())
			}
		}

		return err
	})

	if cacheErr != nil {
		log.Printf("[WARN] getRowsSince failed to read page %d key %s after %d retries: %s (%s)", pageIdx, pageKey, maxRetries, cacheErr.Error(), req.CallId)
		return nil, cacheErr
	}

	log.Printf("[INFO] getRowsSince read page %d after %d %s  - key %s (%s)",
		pageIdx,
		retries,
		pluralize.NewClient().Pluralize("retry", retries, false),
		pageKey,
		req.CallId)

	return cachedResult, nil
}

func (req *setRequest) waitForSubscribers(ctx context.Context) {
	log.Printf("[INFO] setRequest waitForSubscribers (%s)", req.CallId)

	doneChan := make(chan struct{})
	go func() {
		for subscriber := range req.subscribers {
			log.Printf("[INFO] waiting for subscriber %s (%s)", subscriber.callId, req.CallId)
			err := subscriber.waitUntilDone()
			if err != nil {
				log.Printf("[INFO] subscriber %s had error: %s (%s)", subscriber.callId, err.Error(), req.CallId)
			} else {
				log.Printf("[INFO] subscriber %s done (%s)", subscriber.callId, req.CallId)
			}
		}
		close(doneChan)
	}()

	select {
	case <-ctx.Done():
	case <-doneChan:
	}
}

func (req *setRequest) addRow(row *sdkproto.Row) error {
	req.requestLock.Lock()
	defer func() {
		req.requestLock.Unlock()
	}()

	// if the request has no subscribers, cancel this scan
	if len(req.subscribers) == 0 {
		// lock access to set request
		log.Printf("[INFO] IterateSet NO SUBSCRIBERS! (%s)", req.CallId)
		return NoSubscribersError{}
	}

	// was there an error in a previous iterate
	if req.err != nil {
		log.Printf("[INFO] IterateSet request is in error: %s (%s)", req.err.Error(), req.CallId)
		return req.err
	}

	// set row
	req.pageBuffer[req.bufferIndex] = row
	// update counts
	req.bufferIndex++
	req.rowCount++

	return nil

}

func getPageKey(resultKeyRoot string, pageIdx int) string {
	return fmt.Sprintf("%s-%d", resultKeyRoot, pageIdx)
}

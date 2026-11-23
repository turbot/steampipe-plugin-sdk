package query_cache

import (
	"context"
	"fmt"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
	"log"
	"sync"
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
	// the query that started the scan: it reads the scan as a subscriber too
	ownSubscriber *setRequestSubscriber
	// set when a page does not fit in the cache: the readers still get every row,
	// but the result is not cached
	notCached bool
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

// subscribe adds a reader. The reader needs every page the scan has written, and reads them
// from the cache, so it pins them. Each page the scan writes after this is pinned for it too.
// The reader cannot join a scan that has failed, or whose pages are not all in the cache:
// then subscribe returns false.
// note: requestLock must be locked when this is called
func (req *setRequest) subscribe(subscriber *setRequestSubscriber) bool {
	if req.state == requestError || !req.cache.cache.pin(req.pageKeys(0, int(req.pageCount))) {
		return false
	}
	req.subscribers[subscriber] = struct{}{}
	return true
}

// unsubscribe removes a reader, and releases its pins on the pages it has not read.
// note: requestLock must be locked when this is called
func (req *setRequest) unsubscribe(subscriber *setRequestSubscriber) {
	if _, ok := req.subscribers[subscriber]; !ok {
		return
	}
	delete(req.subscribers, subscriber)
	req.cache.cache.unpin(req.pageKeys(subscriber.pinnedFrom, int(req.pageCount))...)
}

// pageKeys returns the cache keys of pages from to to-1.
func (req *setRequest) pageKeys(from, to int) []string {
	keys := make([]string, 0, max(to-from, 0))
	for pageIdx := from; pageIdx < to; pageIdx++ {
		keys = append(keys, getPageKey(req.resultKeyRoot, pageIdx))
	}
	return keys
}

func (req *setRequest) getBufferedRows() []*sdkproto.Row {
	if req.bufferIndex == 0 {
		return nil
	}
	return req.pageBuffer[:req.bufferIndex]
}

// return all rows available after the given row count
func (req *setRequest) getRowsSince(rowsAlreadyStreamed int) ([]*sdkproto.Row, error) {
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

	// so we must load data from the cache.
	// Return one page per call - the subscriber calls again for the rest.
	// (Loading every page since startPage at once held a lagging subscriber's whole backlog
	// in memory, which for a large table is hundreds of MB, while holding the request lock.)
	pageKey := getPageKey(req.resultKeyRoot, startPage)
	cachedResult := &sdkproto.QueryResult{}
	if err := doGet(pageKey, req.cache.cache, cachedResult); err != nil {
		log.Printf("[WARN] getRowsSince failed to read page %d key %s: %s (%s)", startPage, pageKey, err.Error(), req.CallId)
		// the subscriber pins every page it has not read, so the page must be there.
		// Do not return a cache miss: the subscriber may have streamed rows already
		if IsCacheMiss(err) {
			return nil, fmt.Errorf("page %d of the scan is missing from the query cache", startPage)
		}
		return nil, err
	}
	res := cachedResult.Rows[startOffset:]

	log.Printf("[TRACE] setRequest getRowsSince returning %d from page %d", len(res), startPage)
	return res, nil
}

// waitForSubscribers waits until each subscriber has stopped reading, or ctx is done.
func (req *setRequest) waitForSubscribers(ctx context.Context) {
	log.Printf("[INFO] setRequest waitForSubscribers (%s)", req.CallId)

	// subscribers remove themselves when they stop, so wait for the ones there are now
	req.requestLock.RLock()
	subscribers := make([]*setRequestSubscriber, 0, len(req.subscribers))
	for subscriber := range req.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	req.requestLock.RUnlock()

	for _, subscriber := range subscribers {
		log.Printf("[INFO] waiting for subscriber %s (%s)", subscriber.callId, req.CallId)
		select {
		case <-ctx.Done():
			return
		case <-subscriber.doneChan:
		}
		if subscriber.err != nil {
			log.Printf("[INFO] subscriber %s had error: %s (%s)", subscriber.callId, subscriber.err.Error(), req.CallId)
		} else {
			log.Printf("[INFO] subscriber %s done (%s)", subscriber.callId, req.CallId)
		}
	}
}

func (req *setRequest) addRow(row *sdkproto.Row) error {
	req.requestLock.Lock()
	defer func() {
		req.requestLock.Unlock()
	}()

	// if the request has no subscribers, cancel this scan
	if len(req.subscribers) == 0 {
		// the query that started the scan stopped reading it because of an error: that query fails
		if req.ownSubscriber != nil && req.ownSubscriber.err != nil {
			return req.ownSubscriber.err
		}
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

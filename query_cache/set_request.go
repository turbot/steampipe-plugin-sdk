package query_cache

import (
	"context"
	"fmt"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
	"sync"
	"sync/atomic"
)

type setRequest struct {
	*CacheRequest
	// other cache requests who are subscribing to this data
	subscribers map[*setRequestSubscriber]struct{}
	requestLock sync.RWMutex
	// flag used by sunscribers when getting rows to stream
	complete   atomic.Bool
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

// send error to subscribers
func (req *setRequest) sendErrorToSubscribers(err error) {
	log.Printf("[WARN] aborting set request with error: %s (%s)", err.Error(), req.CallId)
	for subscriber := range req.subscribers {
		subscriber.errChan <- err
	}
	log.Printf("[INFO] done aborting")
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

// return all rows available aftyer the given row count
func (req *setRequest) getRowsSince(ctx context.Context, rowsAlreadyStreamed int) ([]*sdkproto.Row, error) {

	/*

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

	log.Printf("[INFO] setRequest getRowsSince rowsAlreadyStreamed: %d, req.pageCount: %d, req.bufferIndex: %d, startPage: %d startOffset: %d, ",
		rowsAlreadyStreamed,
		req.pageCount,
		req.bufferIndex,
		startPage,
		startOffset)

	// if start page is the current page, this means we do not need any data written to the cache
	// just return rows from page buffer
	if startPage == int(req.pageCount) {
		log.Printf("[INFO] setRequest getRowsSince returning %d from buffer", req.bufferIndex-startOffset)
		bufferedRows := req.getBufferedRows()
		// apply the start offset
		return bufferedRows[startOffset:], nil
	}

	// so we must load some data from the cache

	// build result rows
	var res = make([]*sdkproto.Row, 0, req.rowCount-rowsAlreadyStreamed)

	// load previously cache pages`
	for pageIdx := startPage; pageIdx < int(req.pageCount); pageIdx++ {
		cachedResult := sdkproto.QueryResult{}

		pageKey := getPageKey(req.resultKeyRoot, pageIdx)
		if err := doGet[*sdkproto.QueryResult](ctx, pageKey, req.cache.cache, &cachedResult); err != nil {
			return nil, err
		}

		// copy rows into result - taking into account start offset if this is the first page
		idx := 0
		if pageIdx == startPage {
			idx = startOffset
		}
		res = append(res, cachedResult.Rows[idx:]...)
	}

	// now add any rows from the page buffer
	res = append(res, req.getBufferedRows()...)

	log.Printf("[INFO] setRequest getRowsSince returning %d", len(res))
	return res, nil
}

func (req *setRequest) waitForSubscribers(ctx context.Context) {
	log.Printf("[INFO] setRequest waitForSubscribers (%s)", req.CallId)

	doneChan := make(chan struct{})
	go func() {
		for subscriber := range req.subscribers {
			log.Printf("[INFO] waiting for subscriber %s (%s)", subscriber.callId, req.CallId)
			// TODO KAI THINK ABOUT ERROR - return error
			subscriber.waitUntilDone()
			log.Printf("[INFO] subscriber %s done (%s)", subscriber.callId, req.CallId)
		}
		close(doneChan)
	}()

	// TODO timeout?
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

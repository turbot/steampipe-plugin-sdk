package query_cache

import (
	"fmt"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
	"sync"
)

type setRequest struct {
	*CacheRequest
	// other cache requests who are subscribing to this data
	subscribers map[*setRequestSubscriber]struct{}
	mut         sync.RWMutex
	complete    bool
	pageBuffer  []*sdkproto.Row
	// index within the page buffer
	pageIndex int
	err       error
}

func newSetRequest(req *CacheRequest) *setRequest {
	return &setRequest{
		CacheRequest: req,
		subscribers:  make(map[*setRequestSubscriber]struct{}),
		pageBuffer:   make([]*sdkproto.Row, rowBufferSize),
	}
}

func (req *setRequest) subscribe(subscriber *setRequestSubscriber) {
	req.subscribers[subscriber] = struct{}{}
	// if we are already complete, tell our subscriber by streaming a nil row
	if req.complete {
		subscriber.streamRowFunc(nil)
	}
}

func (req *setRequest) unsubscribe(subscriber *setRequestSubscriber) {
	delete(req.subscribers, subscriber)
}

func (req *setRequest) streamToSubscribers(row *sdkproto.Row) {
	log.Printf("[WARN] streamToSubscribers (%s)", req.CallId)
	for subscriber := range req.subscribers {
		go func() {
			// if subscriber is stiull waiting to complete streamining previous rows
			// (maybe it's channel is blocked)
			// skip for now - we will try again when the next row is set
			if !subscriber.streamLock.TryLock() {
				return
			}
			defer subscriber.streamLock.Unlock()

			// figure out how may rows we need to stream to the subscriber
			// (it may be reading at a slower rate than the rows are being written so there may be a backlog)
			//rowsToStream := req.rowCount() - subscriber.rowsStreamed

			// stream the row
			subscriber.streamRowFunc(row)
		}()
	}
}

// send error to subscribers
func (req *setRequest) abort(err error) {
	log.Printf("[WARN] aborting set request with error: %s (%s)", err.Error(), req.CallId)
	for subscriber := range req.subscribers {
		subscriber.errChan <- err
	}
	log.Printf("[WARN] done aborting")
}

func (req *setRequest) getBufferedRows() []*sdkproto.Row {
	if req.pageIndex == 0 {
		return nil
	}
	return req.pageBuffer[:req.pageIndex]
}

func (req *setRequest) rowCount() int {
	return int(req.pageCount)*rowBufferSize + req.pageIndex
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

func getPageKey(resultKeyRoot string, pageIdx int) string {
	return fmt.Sprintf("%s-%d", resultKeyRoot, pageIdx)
}

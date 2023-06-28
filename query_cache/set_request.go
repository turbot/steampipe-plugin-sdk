package query_cache

import (
	"fmt"
	"github.com/turbot/steampipe-plugin-sdk/v5/error_helpers"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
	"sync"
)

type setRequest struct {
	*CacheRequest
	// other cache requests who are subscribing to this data
	subscribers map[*setRequestSubscriber]struct{}
	requestLock sync.RWMutex
	// TODO KAI THINK ABOUT THIS
	complete   bool
	pageBuffer []*sdkproto.Row
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
	// note: requestLock must be locked when this is called
	req.subscribers[subscriber] = struct{}{}
}

func (req *setRequest) unsubscribe(subscriber *setRequestSubscriber) {
	// note: requestLock must be locked when this is called
	delete(req.subscribers, subscriber)
}

func (req *setRequest) streamToSubscribers(row *sdkproto.Row) {
	log.Printf("[INFO] streamToSubscribers (%s)", req.CallId)
	for subscriber := range req.subscribers {

		// check if subscriber is still waiting to complete streamining previous rows
		// (maybe it's channel is blocked...)
		// if this is NOT the final row (i.e. row is not nil) skip for now - we will try again when the next row is set
		// if this is IS the final row (i.e. row is nil) wait for it
		if row == nil {
			log.Printf("[INFO] setRequest streamToSubscribers sending nil row to subscribers - waiting to acquire stream lock for %s (%s)", subscriber.callId, req.CallId)
			subscriber.streamLock.Lock()
			log.Printf("[INFO] acquired stream lock for %s (%s)", subscriber.callId, req.CallId)
		} else if !subscriber.streamLock.TryLock() {
			continue
		}

		go func(s *setRequestSubscriber) {

			defer s.streamLock.Unlock()

			// figure out how may rows we need to stream to the subscriber
			// (it may be reading at a slower rate than the rows are being written so there may be a backlog)
			//rowsToStream := req.rowCount() - subscriber.rowsStreamed

			// stream the row
			err := s.streamRowFunc(row)
			// if this returns a context cancelled error, unsubscribe
			if error_helpers.IsContextCancelledError(err) {
				req.requestLock.Lock()
				log.Printf("[INFO] subscriber %s returned context canclled - unsubscribing (%s)", s.callId, req.CallId)
				req.unsubscribe(s)
				req.requestLock.Unlock()
			}
		}(subscriber)
	}
}

// send error to subscribers
func (req *setRequest) sendErrorToSubscribers(err error) {
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

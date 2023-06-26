package query_cache

import (
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
}

func newSetRequest(req *CacheRequest) *setRequest {
	return &setRequest{
		CacheRequest: req,
		subscribers:  make(map[*setRequestSubscriber]struct{}),
	}
}

func (r *setRequest) subscribe(subscriber *setRequestSubscriber) {
	r.subscribers[subscriber] = struct{}{}
	// if we are already complete, tell our subscriber by streaming a nil row
	if r.complete {
		subscriber.streamRowFunc(nil)
	}
}
func (r *setRequest) unsubscribe(subscriber *setRequestSubscriber) {
	delete(r.subscribers, subscriber)
}

func (r *setRequest) streamToSubscribers(row *sdkproto.Row) {
	for subscriber := range r.subscribers {
		subscriber.streamRowFunc(row)
	}
}

// send error to subscribers
func (r *setRequest) abort(err error) {
	log.Printf("[WARN] aborting set request with error: %s (%s)", err.Error(), r.CallId)
	for subscriber := range r.subscribers {
		subscriber.errChan <- err
	}
	log.Printf("[WARN] done aborting")
}

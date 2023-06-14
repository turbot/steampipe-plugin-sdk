package query_cache

import (
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"sync"
)

type setRequest struct {
	*CacheRequest
	// other cache requests who are subscribing to this data
	subscribers []*setRequestSubscriber
	mut         sync.RWMutex
	complete    bool
}

func (r *setRequest) subscribe(subscriber *setRequestSubscriber) {
	r.subscribers = append(r.subscribers, subscriber)
	// if we are already complete, tell our subscriber by streaming a nil row
	if r.complete {
		subscriber.streamRowFunc(nil)
	}
}

func (r *setRequest) streamToSubscribers(row *sdkproto.Row) {
	for _, subscriber := range r.subscribers {
		subscriber.streamRowFunc(row)
	}
}

// send error to subscribers
func (r *setRequest) abort(err error) {
	for _, subscriber := range r.subscribers {
		subscriber.errChan <- err
	}
}

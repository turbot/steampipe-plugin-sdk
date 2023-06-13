package query_cache

import (
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"sync"
)

type setRequest struct {
	*CacheRequest
	// other cache requests who are subscribing to this data
	subscribers []func(row *sdkproto.Row)
	mut         sync.RWMutex
	complete    bool
}

func (r *setRequest) subscribe(subscriber func(row *sdkproto.Row)) {
	r.subscribers = append(r.subscribers, subscriber)
}

func (r *setRequest) streamToSubscribers(row *sdkproto.Row) {
	for _, streamFunc := range r.subscribers {
		streamFunc(row)
	}
}

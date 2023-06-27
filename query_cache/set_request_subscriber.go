package query_cache

import (
	"context"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
	"sync"
)

type setRequestSubscriber struct {
	streamRowFunc func(row *sdkproto.Row) error
	errChan       chan error
	doneChan      chan struct{}
	callId        string
	rowsStreamed  int
	//lastStreamTime time.Time
	publisher *setRequest
	// lock to indicate we are waiting to stream data to our client channel
	streamLock sync.Mutex
	// this flag is set to true when the set request has finished and this subscriber was locked at the time,
	// meaning there may be additional rows available to strem
	// (this is required as normallt we rely on the next iterateSet call to stream any missed rows
	// but iterateSet will not be called again)
	recheck       bool
	streamContext context.Context
}

func newSetRequestSubscriber(streamRowFunc func(row *sdkproto.Row), callId string, streamContext context.Context, publisher *setRequest) *setRequestSubscriber {
	//  we start a goroutine to stream all rows
	doneChan := make(chan struct{})
	errChan := make(chan error, 1)

	s := &setRequestSubscriber{
		errChan:       errChan,
		doneChan:      doneChan,
		callId:        callId,
		publisher:     publisher,
		streamContext: streamContext,
		//lastStreamTime: time.Now(),
	}

	wrappedStreamFunc := func(row *sdkproto.Row) error {
		if row == nil {
			log.Printf("[INFO] null row, closing doneChan (%s)", callId)
			close(doneChan)
			return nil
		}

		var streamedRowChan = make(chan struct{})
		go func() {
			streamRowFunc(row)
			//s.lastStreamTime = time.Now()
			s.rowsStreamed++
			close(streamedRowChan)
		}()

		select {
		// first check for context cancellation - this may happen if channel is blocked and scane is subsequently cancelled
		case <-s.streamContext.Done():
			// close the done chan
			close(doneChan)
			// return the context error
			return s.streamContext.Err()
		case <-streamedRowChan:
			return nil
		}
	}

	s.streamRowFunc = wrappedStreamFunc

	return s
}

func (s *setRequestSubscriber) waitUntilDone() (err error) {
	//defer func() {
	// wrap error in a subscriber error
	//if err != nil {
	//	err = newSubscriberError(err, s)
	//}
	//}()
	select {
	case <-s.doneChan:
		return nil
	case err := <-s.errChan:
		log.Printf("[WARN] setRequestSubscriber received an error from setRequest %s: %s (%s)", s.publisher.CallId, err.Error(), s.callId)
		return err
	}

}

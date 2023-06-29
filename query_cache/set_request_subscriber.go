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
	// the set request we receiving data from
	publisher *setRequest
	// lock to indicate we are waiting to stream data to our client channel
	streamLock sync.Mutex
	// the context of the execute request - use to check for cancellation
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
	}

	// asyncronously stream a row, while checing for cancellation
	// (cancellation means Postgres has called EndForeighnScan)
	wrappedStreamFunc := func(row *sdkproto.Row) error {
		if row == nil {
			log.Printf("[INFO] null row, closing doneChan (%s)", callId)
			close(doneChan)
			return nil
		}

		// TODO KAI verify goroutines are cleaned up

		var streamedRowChan = make(chan struct{})
		go func() {
			streamRowFunc(row)
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
	select {
	case <-s.doneChan:
		return nil
	case err := <-s.errChan:
		log.Printf("[WARN] setRequestSubscriber received an error from setRequest %s: %s (%s)", s.publisher.CallId, err.Error(), s.callId)
		return err
	}

}

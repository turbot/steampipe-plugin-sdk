package query_cache

import (
	"context"
	"fmt"
	"github.com/sethvargo/go-retry"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
	"time"
)

type setRequestSubscriber struct {
	streamRowFunc func(row *sdkproto.Row)
	errChan       chan error
	doneChan      chan struct{}
	callId        string
	rowsStreamed  int
	// the set request we receiving data from
	publisher *setRequest
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
		streamRowFunc: streamRowFunc,
	}

	return s
}

// asyncronously loop reading rows until scan is complete or cancelled
func (s *setRequestSubscriber) readRowsAsync(ctx context.Context) {
	go func() {
		log.Printf("[INFO] setRequestSubscriber readRowsAsync publisher %s (%s)", s.publisher.CallId, s.callId)
		defer func() {
			close(s.doneChan)
			log.Printf("[INFO] setRequestSubscriber readRowsAsync goroutine returning - doneChan CLOSED - publisher %s (%s)", s.publisher.CallId, s.callId)
		}()
		var streamChan = make(chan struct{})

		baseRetryInterval := 1 * time.Millisecond
		maxRetryInterval := 50 * time.Millisecond
		backoff := retry.WithCappedDuration(maxRetryInterval, retry.NewExponential(baseRetryInterval))

		go func() {
			defer close(streamChan)
			for {

				var rowsTostream []*sdkproto.Row

				err := retry.Do(ctx, backoff, func(ctx context.Context) error {
					var getRowsErr error
					log.Printf("[INFO] readRowsAsync getting rowsTostream (%s)", s.callId)
					rowsTostream, getRowsErr = s.getRowsToStream(ctx)
					log.Printf("[INFO] readRowsAsync rowsTostream %d (%s)", len(rowsTostream), s.callId)
					return getRowsErr
				})

				// getRowsToStream will keep retrying as long as there are still rows to stream (or there is an error)
				if len(rowsTostream) == 0 {
					if err != nil {
						// TODO KAI HOW TO RAISE?
						log.Printf("[WARN] readRowsAsync failed to read previous rows from cache: %s", err)
					}
					log.Printf("[INFO] readRowsAsync no more rows to stream - publisher %s (%s)", s.publisher.CallId, s.callId)
					return
				}

				log.Printf("[INFO] readRowsAsync stream %d (%s)", len(rowsTostream), s.callId)

				for _, row := range rowsTostream {
					s.streamRowFunc(row)
					s.rowsStreamed++
					// check for contect cancellation
					if s.streamContext.Err() != nil {
						log.Printf("[INFO] readRowsAsync stream context cancelled (%s)", s.callId)
						return
					}
				}

				log.Printf("[INFO] readRowsAsync streaming complete (%s)", s.callId)
			}

		}()

		// wait for all rows tro be streams (or cancellation)
		select {
		// first check for context cancellation - this may happen if channel is blocked and scane is subsequently cancelled
		case <-s.streamContext.Done():
			log.Printf("[INFO] readRowsAsync stream context was cancelled - publisher %s (%s)", s.publisher.CallId, s.callId)
			// unsubscribe from publisher
			s.publisher.requestLock.Lock()
			s.publisher.unsubscribe(s)
			s.publisher.requestLock.Unlock()
		// are we done streaming?
		case <-streamChan:
			log.Printf("[INFO] readRowsAsync finished streaming - publisher %s (%s)", s.publisher.CallId, s.callId)
		// was there an error
		case <-s.errChan:
			log.Printf("[INFO] readRowsAsync error received - publisher %s (%s)", s.publisher.CallId, s.callId)
		}
	}()
}

func (s *setRequestSubscriber) getRowsToStream(ctx context.Context) ([]*sdkproto.Row, error) {
	log.Printf("[INFO] setRequestSubscriber getting lock (%s)", s.callId)
	s.publisher.requestLock.RLock()
	log.Printf("[INFO] setRequestSubscriber got lock (%s)", s.callId)
	rowsTostream, err := s.publisher.getRowsSince(ctx, s.rowsStreamed)

	s.publisher.requestLock.RUnlock()
	log.Printf("[INFO] setRequestSubscriber released lock (%s)", s.callId)

	if err != nil {
		return nil, err
	}

	if len(rowsTostream) == 0 {
		if s.publisher.complete {
			log.Printf("[INFO] getRowsToStream - publisher %s complete - returning (%s)", s.publisher.CallId, s.callId)
			return nil, nil
		}
		// if no rows are available, retry
		return nil, retry.RetryableError(fmt.Errorf("no rows available to stream"))
	}

	// ok we have rows
	return rowsTostream, nil
}

func (s *setRequestSubscriber) waitUntilDone() error {
	// TODO KAI this crash causes plugin manager to shutdown????
	//log.Printf("[WARN] waitUntilDone received an error from setRequest %s: %s (%s)", s.publisher.CallId, err.Error(), s.callId)
	log.Printf("[INFO] waitUntilDone  (%s)", s.callId)
	select {
	case <-s.doneChan:
		log.Printf("[INFO] <-s.doneChan (%s)", s.callId)
		return nil
	case err := <-s.errChan:
		log.Printf("[WARN] setRequestSubscriber received an error from setRequest %s: %s (%s)", s.publisher.CallId, err.Error(), s.callId)
		return err
	}
}

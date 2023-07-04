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
			// indicate this subscriber has finished
			close(s.doneChan)
			log.Printf("[INFO] setRequestSubscriber readRowsAsync goroutine returning - doneChan CLOSED - publisher %s (%s)", s.publisher.CallId, s.callId)
		}()

		// internal goroutine to read all rows from the publisher and stream them
		streamChan, errChan := s.readAndStreamAsync(ctx)

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
		case err := <-errChan:
			log.Printf("[INFO] readRowsAsync error received: %s - publisher %s (%s)", err.Error(), s.publisher.CallId, s.callId)
			s.errChan <- err
		}
	}()
}

func (s *setRequestSubscriber) readAndStreamAsync(ctx context.Context) (chan struct{}, chan error) {
	// channel to indicate all rows have been streamed
	var streamChan = make(chan struct{})
	var errChan = make(chan error, 1)

	go func() {
		defer close(streamChan)

		baseRetryInterval := 1 * time.Millisecond
		maxRetryInterval := 50 * time.Millisecond
		backoff := retry.WithCappedDuration(maxRetryInterval, retry.NewExponential(baseRetryInterval))

		for {
			log.Printf("[TRACE] readRowsAsync internal goroutine to read all rows from the publisher and stream them (rows streamed %d) (%s)", s.rowsStreamed, s.callId)

			var rowsTostream []*sdkproto.Row

			// get rows available to stream - retry with backoff
			err := retry.Do(ctx, backoff, func(ctx context.Context) error {
				var getRowsErr error
				log.Printf("[TRACE] readRowsAsync getting rowsTostream (rows streamed %d) (%s)", s.rowsStreamed, s.callId)
				rowsTostream, getRowsErr = s.getRowsToStream(ctx)
				log.Printf("[TRACE] readRowsAsync rowsTostream %d (%s)", len(rowsTostream), s.callId)
				return getRowsErr
			})

			log.Printf("[TRACE] readRowsAsync retry returned %d rows to stream (%s)", len(rowsTostream), s.callId)

			// getRowsToStream will keep retrying as long as there are still rows to stream or there is an error
			if len(rowsTostream) == 0 {
				// either there is an error, or we are done... check which it is
				if err != nil {
					log.Printf("[WARN] readRowsAsync failed to read previous rows from cache: %s publisher %s (%s)", err, s.publisher.CallId, s.callId)
					errChan <- err
				}
				// to get here, publisdher has no more rows
				// exit the goroutine
				return
			}

			for _, row := range rowsTostream {

				s.streamRowFunc(row)
				s.rowsStreamed++
				// check for contect cancellation
				if s.streamContext.Err() != nil {
					log.Printf("[INFO] readRowsAsync stream context cancelled (%s)", s.callId)
					errChan <- s.streamContext.Err()
					return
				}
			}

			log.Printf("[TRACE] readRowsAsync streaming complete (rows streamed %d) (%s)", s.rowsStreamed, s.callId)
		}

	}()
	return streamChan, errChan
}

func (s *setRequestSubscriber) getRowsToStream(ctx context.Context) ([]*sdkproto.Row, error) {
	s.publisher.requestLock.RLock()
	rowsTostream, err := s.publisher.getRowsSince(ctx, s.rowsStreamed)
	s.publisher.requestLock.RUnlock()

	if err != nil {
		log.Printf("[INFO] getRowsToStream getRowsSince returned error: %s (%s)", err.Error(), s.callId)
		return nil, err
	}

	if len(rowsTostream) == 0 {
		if s.publisher.complete.Load() {
			log.Printf("[INFO] getRowsToStream - publisher %s complete - returning (%s)", s.publisher.CallId, s.callId)
			return nil, nil
		}
		// if no rows are available, retry
		// (this is called from within a retry.Do)
		return nil, retry.RetryableError(fmt.Errorf("no rows available to stream"))
	}

	log.Printf("[TRACE] getRowsToStream returning %d (%s)", len(rowsTostream), s.callId)
	// ok we have rows
	return rowsTostream, nil
}

func (s *setRequestSubscriber) waitUntilDone() error {
	// TODO KAI this crash causes plugin manager to shutdown????
	//log.Printf("[WARN] waitUntilDone received an error from setRequest %s: %s (%s)", s.publisher.CallId, err.Error(), s.callId)
	log.Printf("[INFO] waitUntilDone  (%s)", s.callId)
	select {
	case <-s.doneChan:
		log.Printf("[INFO] waitUntilDone - doneChan signalled - we're done (%s)", s.callId)
		return nil
	case err := <-s.errChan:
		log.Printf("[WARN] waitUntilDone - setRequestSubscriber received an error from setRequest %s: %s (%s)", s.publisher.CallId, err.Error(), s.callId)
		return err
	}
}

// wait until this subscriber has streamed all available rows
func (s *setRequestSubscriber) waitUntilAvailableRowsStreamed(ctx context.Context, availableRows int) {
	log.Printf("[TRACE] waitUntilAvailableRowsStreamed (%s)", s.callId)
	defer log.Printf("[TRACE] waitUntilAvailableRowsStreamed done(%s)", s.callId)
	baseRetryInterval := 1 * time.Millisecond
	maxRetryInterval := 50 * time.Millisecond
	backoff := retry.WithCappedDuration(maxRetryInterval, retry.NewExponential(baseRetryInterval))

	// we know this cannot return an error
	_ = retry.Do(ctx, backoff, func(ctx context.Context) error {
		// if context is cancelled just return
		if ctx.Err() != nil || s.streamContext.Err() != nil {
			log.Printf("[INFO] waitUntilAvailableRowsStreamed context cancelled - returning (%s)", s.callId)

			return nil
		}

		if s.rowsStreamed < availableRows {
			return retry.RetryableError(fmt.Errorf("not all available rows streamed"))
		}
		return nil
	})
}

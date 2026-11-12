package query_cache

import (
	"context"
	"fmt"
	"github.com/sethvargo/go-retry"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
	"log"
	"time"
)

type setRequestSubscriber struct {
	streamRowFunc func(row *sdkproto.Row)
	// closed when the subscriber stops reading
	doneChan chan struct{}
	// the error the subscriber stopped with, if any. Set under the publisher's requestLock,
	// before doneChan is closed
	err          error
	callId       string
	rowsStreamed int
	// the first page this subscriber still pins: it pins every page from here to the
	// publisher's page count. Changed under the publisher's requestLock (a read lock is enough,
	// as only the subscriber's own reading goroutine changes it)
	pinnedFrom int
	// the set request we receiving data from
	publisher *setRequest
	// the context of the execute request - use to check for cancellation
	streamContext context.Context
}

func newSetRequestSubscriber(streamRowFunc func(row *sdkproto.Row), callId string, streamContext context.Context, publisher *setRequest) *setRequestSubscriber {
	return &setRequestSubscriber{
		doneChan:      make(chan struct{}),
		callId:        callId,
		publisher:     publisher,
		streamContext: streamContext,
		streamRowFunc: streamRowFunc,
	}
}

// asyncronously loop reading rows until scan is complete or cancelled
func (s *setRequestSubscriber) readRowsAsync(ctx context.Context) {
	go func() {
		log.Printf("[INFO] setRequestSubscriber readRowsAsync publisher %s (%s)", s.publisher.CallId, s.callId)
		var err error
		defer func() {
			// stop subscribing, which releases our pins, then indicate this subscriber has finished
			s.publisher.requestLock.Lock()
			s.err = err
			s.publisher.unsubscribe(s)
			s.publisher.requestLock.Unlock()
			close(s.doneChan)
			log.Printf("[INFO] setRequestSubscriber readRowsAsync goroutine returning - doneChan CLOSED - publisher %s (%s)", s.publisher.CallId, s.callId)
		}()

		// internal goroutine to read all rows from the publisher and stream them
		streamDone := s.readAndStreamAsync(ctx)

		// wait for all rows to be streams (or cancellation)
		select {
		// first check for context cancellation - this may happen if channel is blocked and scane is subsequently cancelled
		case <-s.streamContext.Done():
			log.Printf("[INFO] readRowsAsync stream context was cancelled - publisher %s (%s)", s.publisher.CallId, s.callId)
		// all rows are streamed, or there was an error
		case err = <-streamDone:
			log.Printf("[INFO] readRowsAsync finished streaming - publisher %s (%s)", s.publisher.CallId, s.callId)
		}
	}()
}

// readAndStreamAsync reads all rows from the publisher and streams them. The channel it
// returns gets nil when all rows are streamed, or the error that stopped it.
func (s *setRequestSubscriber) readAndStreamAsync(ctx context.Context) chan error {
	var streamDone = make(chan error, 1)

	go func() {
		baseRetryInterval := 1 * time.Millisecond
		maxRetryInterval := 50 * time.Millisecond
		backoff := retry.WithCappedDuration(maxRetryInterval, retry.NewExponential(baseRetryInterval))

		for {
			log.Printf("[TRACE] readAndStreamAsync internal goroutine to read all rows from the publisher and stream them (rows streamed %d) (%s)", s.rowsStreamed, s.callId)

			var rowsTostream []*sdkproto.Row

			// get rows available to stream - retry with backoff
			err := retry.Do(ctx, backoff, func(ctx context.Context) error {
				var getRowsErr error
				log.Printf("[TRACE] readAndStreamAsync getting rowsTostream (rows streamed %d) (%s)", s.rowsStreamed, s.callId)
				rowsTostream, getRowsErr = s.getRowsToStream()
				log.Printf("[TRACE] readAndStreamAsync rowsTostream %d (%s)", len(rowsTostream), s.callId)
				return getRowsErr
			})

			log.Printf("[TRACE] readAndStreamAsync retry returned %d rows to stream (%s)", len(rowsTostream), s.callId)

			// is there an error
			if err != nil {
				log.Printf("[WARN] readAndStreamAsync failed to read previous rows from cache: %s publisher %s (%s)", err, s.publisher.CallId, s.callId)
				streamDone <- err
				return
			}

			// getRowsToStream will keep retrying as long as there are still rows to stream (or there is an error)
			if len(rowsTostream) == 0 {
				log.Printf("[TRACE] readAndStreamAsync returning")
				// to get here, publisher has no more rows
				// exit the goroutine
				streamDone <- nil
				return
			}

			for _, row := range rowsTostream {
				log.Printf("[TRACE] readAndStreamAsync stream row (%s)", s.callId)
				s.streamRowFunc(row)
				s.rowsStreamed++
				// check for contect cancellation
				if s.streamContext.Err() != nil {
					log.Printf("[INFO] readAndStreamAsync stream context cancelled (%s)", s.callId)
					streamDone <- s.streamContext.Err()
					return
				}
			}

			log.Printf("[TRACE] readAndStreamAsync streaming complete (rows streamed %d) (%s)", s.rowsStreamed, s.callId)
		}

	}()
	return streamDone
}

func (s *setRequestSubscriber) getRowsToStream() ([]*sdkproto.Row, error) {
	s.publisher.requestLock.RLock()
	defer s.publisher.requestLock.RUnlock()

	if s.publisher.state == requestError {
		return nil, s.publisher.err
	}

	s.unpinReadPages()
	rowsToStream, err := s.publisher.getRowsSince(s.rowsStreamed)
	if err != nil {
		log.Printf("[INFO] getRowsToStream getRowsSince returned error: %s (%s)", err.Error(), s.callId)
		return nil, err
	}

	if len(rowsToStream) == 0 {
		if s.publisher.state == requestComplete {
			// we are done!
			log.Printf("[INFO] getRowsToStream - publisher %s complete - returning (%s)", s.publisher.CallId, s.callId)
			return nil, nil
		}

		// if no rows are available, retry
		// (NOTE: we have already checked for completiomn
		// (this is called from within a retry.Do)
		return nil, retry.RetryableError(fmt.Errorf("no rows available to stream"))
	}

	log.Printf("[TRACE] getRowsToStream returning %d (%s)", len(rowsToStream), s.callId)
	// ok we have rows
	return rowsToStream, nil
}

// unpinReadPages releases the pins on the pages this subscriber has read.
// note: the publisher's requestLock must be held
func (s *setRequestSubscriber) unpinReadPages() {
	if _, ok := s.publisher.subscribers[s]; !ok {
		// unsubscribe released the pins
		return
	}
	readPages := min(s.rowsStreamed/rowBufferSize, int(s.publisher.pageCount))
	if readPages > s.pinnedFrom {
		s.publisher.cache.cache.unpin(s.publisher.pageKeys(s.pinnedFrom, readPages)...)
		s.pinnedFrom = readPages
	}
}

// waitUntilDone waits until the subscriber stops reading, and returns the error it stopped with, if any.
func (s *setRequestSubscriber) waitUntilDone() error {
	log.Printf("[INFO] waitUntilDone  (%s)", s.callId)
	<-s.doneChan
	if s.err != nil {
		log.Printf("[WARN] waitUntilDone - setRequestSubscriber received an error from setRequest %s: %s (%s)", s.publisher.CallId, s.err.Error(), s.callId)
		return s.err
	}
	log.Printf("[INFO] waitUntilDone - doneChan signalled - we're done (%s)", s.callId)
	return nil
}

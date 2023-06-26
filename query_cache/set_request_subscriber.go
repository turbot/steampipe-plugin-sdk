package query_cache

import (
	"fmt"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
	"time"
)

type setRequestSubscriber struct {
	streamRowFunc  func(row *sdkproto.Row)
	errChan        chan error
	doneChan       chan struct{}
	callId         string
	rowsStreamed   int
	lastStreamTime time.Time
	publisher      *setRequest
}

func newSetRequestSubscriber(streamRowFunc func(row *sdkproto.Row), callId string, publisher *setRequest) *setRequestSubscriber {
	//  we start a goroutine to stream all rows
	doneChan := make(chan struct{})
	errChan := make(chan error, 1)

	s := &setRequestSubscriber{
		errChan:        errChan,
		doneChan:       doneChan,
		callId:         callId,
		publisher:      publisher,
		lastStreamTime: time.Now(),
	}

	wrappedStreamFunc := func(row *sdkproto.Row) {
		if row == nil {
			log.Printf("[INFO] null row, closing doneChan (%s)", callId)
			close(doneChan)
			return
		}

		streamRowFunc(row)
		s.lastStreamTime = time.Now()
		s.rowsStreamed++
	}

	s.streamRowFunc = wrappedStreamFunc

	return s
}

func (s *setRequestSubscriber) waitUntilDone() (err error) {
	defer func() {
		// wrap error in a subscriber error
		if err != nil {
			err = newSubscriberError(err, s)
		}
	}()
	const streamTimeout = 2 * time.Minute
	for {
		select {
		//check every 30 secs for stream timeout
		case <-time.After(30 * time.Second):
			if time.Since(s.lastStreamTime) > streamTimeout {
				log.Printf("[WARN] timed out after %ds waiting for pending item to stream a row (%s)", int(streamTimeout.Seconds()), s.callId)
				// unsubscribe
				log.Printf("[INFO] unsubscribing from publisher %s (%s)", s.publisher.CallId, s.callId)
				s.publisher.mut.Lock()
				s.publisher.unsubscribe(s)
				s.publisher.mut.Unlock()
				return fmt.Errorf("timed out after %ds waiting for pending item to stream a row", int(streamTimeout.Seconds()))

			}
		case <-s.doneChan:
			return nil
		case err := <-s.errChan:
			log.Printf("[WARN] setRequestSubscriber received an error from setRequest %s: %s (%s)", s.publisher.CallId, err.Error(), s.callId)
			return err
		}
	}
}

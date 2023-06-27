package query_cache

import (
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
	"sync"
)

type setRequestSubscriber struct {
	streamRowFunc func(row *sdkproto.Row)
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
	recheck bool
}

func newSetRequestSubscriber(streamRowFunc func(row *sdkproto.Row), callId string, publisher *setRequest) *setRequestSubscriber {
	//  we start a goroutine to stream all rows
	doneChan := make(chan struct{})
	errChan := make(chan error, 1)

	s := &setRequestSubscriber{
		errChan:   errChan,
		doneChan:  doneChan,
		callId:    callId,
		publisher: publisher,
		//lastStreamTime: time.Now(),
	}

	wrappedStreamFunc := func(row *sdkproto.Row) {
		if row == nil {
			log.Printf("[INFO] null row, closing doneChan (%s)", callId)
			close(doneChan)
			return
		}

		streamRowFunc(row)
		//s.lastStreamTime = time.Now()
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
	select {
	case <-s.doneChan:
		return nil
	case err := <-s.errChan:
		log.Printf("[WARN] setRequestSubscriber received an error from setRequest %s: %s (%s)", s.publisher.CallId, err.Error(), s.callId)
		return err
	}

}

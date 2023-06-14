package query_cache

import (
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
)

type setRequestSubscriber struct {
	streamRowFunc func(row *sdkproto.Row)
	errChan       chan error
	doneChan      chan struct{}
	callId        string
}

func newSetRequestSubscriber(streamRowFunc func(row *sdkproto.Row), callId string) *setRequestSubscriber {
	//  we start a goroutine to stream all rows
	doneChan := make(chan struct{})
	errChan := make(chan error, 1)

	wrappedStreamFunc := func(row *sdkproto.Row) {
		if row == nil {
			log.Printf("[INFO] null row, closing doneChan (%s)", callId)
			close(doneChan)
			return
		}
		streamRowFunc(row)
	}

	return &setRequestSubscriber{
		streamRowFunc: wrappedStreamFunc,
		errChan:       errChan,
		doneChan:      doneChan,
		callId:        callId,
	}
}

func (s setRequestSubscriber) waitUntilDone() error {
	// TODO consider timout since last streamed item
	select {
	case <-s.doneChan:
		return nil
	case err := <-s.errChan:
		log.Printf("[WARN] setRequestSubscriber received an error from setRequest: %s (%s)", err.Error(), s.callId)
		return err
	}
}

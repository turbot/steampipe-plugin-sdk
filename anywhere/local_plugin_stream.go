package anywhere

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

const localPluginStreamBuffer = 1024

// LocalPluginStream implements the Sender and Receiver interfaces for local plugin connections.
type LocalPluginStream struct {
	ctx  context.Context
	rows chan *proto.ExecuteResponse
	err  error
	// this channel is closed whenever the local plugin stream receives its first row or error
	// this is to make sure either it waits for the first row or error before returning from Recv
	ready chan struct{}
}

func NewLocalPluginStream(ctx context.Context) *LocalPluginStream {
	return &LocalPluginStream{
		ctx:   ctx,
		rows:  make(chan *proto.ExecuteResponse, localPluginStreamBuffer),
		ready: make(chan struct{}, 1),
	}
}
func (s *LocalPluginStream) Send(r *proto.ExecuteResponse) error {
	// make the ready channel
	defer func() {
		select {
		case <-s.ready:
			//do nothing
		default:
			close(s.ready)
		}
	}()
	s.rows <- r
	return nil
}

func (s *LocalPluginStream) Error(err error) {
	// make the ready channel
	defer func() {
		select {
		case <-s.ready:
			//do nothing
		default:
			close(s.ready)
		}
	}()
	s.err = err
}

func (s *LocalPluginStream) Recv() (*proto.ExecuteResponse, error) {
	// wait for the first row or error
	<-s.ready

	// if an error has been sent, return it
	if err := s.err; err != nil {
		s.err = nil
		// reset the ready channel so that the next call to Recv will wait for the next row or error
		s.ready = make(chan struct{}, 1)
		return nil, err
	}
	resp := <-s.rows
	// reset the ready channel so that the next call to Recv will wait for the next row or error
	if resp == nil {
		s.ready = make(chan struct{}, 1)
	}
	return resp, nil
}

func (s *LocalPluginStream) Context() context.Context {
	return s.ctx
}

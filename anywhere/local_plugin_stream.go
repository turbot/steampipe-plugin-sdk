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
	// closed when the stream is done
	done chan struct{}
}

func NewLocalPluginStream(ctx context.Context) *LocalPluginStream {
	return &LocalPluginStream{
		ctx:   ctx,
		rows:  make(chan *proto.ExecuteResponse, localPluginStreamBuffer),
		ready: make(chan struct{}, 1),
		done:  make(chan struct{}),
	}
}

func (s *LocalPluginStream) Send(r *proto.ExecuteResponse) error {
	select {
	case <-s.done:
		return nil
	default:
		// make the ready channel
		select {
		case <-s.ready:
			// already closed
		default:
			close(s.ready)
		}
		select {
		case s.rows <- r:
			return nil
		case <-s.done:
			return nil
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

func (s *LocalPluginStream) Error(err error) {
	select {
	case <-s.done:
		return
	default:
		s.err = err
		// Ensure ready channel is closed
		select {
		case <-s.ready:
			// already closed
		default:
			close(s.ready)
		}
		close(s.done)
	}
}

func (s *LocalPluginStream) Recv() (*proto.ExecuteResponse, error) {
	// Check for error first
	if err := s.err; err != nil {
		s.err = nil
		return nil, err
	}

	// Check if done
	select {
	case <-s.done:
		return nil, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	default:
	}

	// Wait for ready or context cancellation
	select {
	case <-s.ready:
		// ready channel closed, proceed
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case <-s.done:
		return nil, nil
	}

	// Check for error again after ready
	if err := s.err; err != nil {
		s.err = nil
		return nil, err
	}

	// Try to get a row
	select {
	case <-s.done:
		return nil, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case resp, ok := <-s.rows:
		if !ok {
			// Channel is closed, we're done
			close(s.done)
			return nil, nil
		}
		// Don't create a new ready channel - this was causing the issue
		// The ready channel should only be used for the first row
		return resp, nil
	}
}

func (s *LocalPluginStream) Context() context.Context {
	return s.ctx
}

// Ready returns a channel that is closed when the stream is ready to receive data
func (s *LocalPluginStream) Ready() <-chan struct{} {
	return s.ready
}

// Rows returns the rows channel for direct access
func (s *LocalPluginStream) Rows() chan *proto.ExecuteResponse {
	return s.rows
}

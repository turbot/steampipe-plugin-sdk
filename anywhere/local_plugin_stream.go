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
}

func NewLocalPluginStream(ctx context.Context) *LocalPluginStream {
	return &LocalPluginStream{
		ctx:  ctx,
		rows: make(chan *proto.ExecuteResponse, localPluginStreamBuffer),
	}
}
func (s *LocalPluginStream) Send(r *proto.ExecuteResponse) error {
	s.rows <- r
	return nil
}

func (s *LocalPluginStream) Error(err error) {
	s.err = err
}

func (s *LocalPluginStream) Recv() (*proto.ExecuteResponse, error) {
	// if an error has been sent, return it
	if err := s.err; err != nil {
		s.err = nil
		return nil, err
	}
	resp := <-s.rows
	return resp, nil
}

func (s *LocalPluginStream) Context() context.Context {
	return s.ctx

}

package plugin

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
)

const localPluginStreamBuffer = 1024

type LocalPluginStream struct {
	ctx  context.Context
	rows chan *proto.ExecuteResponse
}

func NewLocalPluginStream(ctx context.Context) *LocalPluginStream {
	return &LocalPluginStream{
		ctx:  ctx,
		rows: make(chan *proto.ExecuteResponse, localPluginStreamBuffer),
	}
}
func (s *LocalPluginStream) Send(r *proto.ExecuteResponse) error {
	log.Printf("[WARN] LocalPluginStream Send")
	s.rows <- r
	return nil
}

func (s *LocalPluginStream) Recv() (*proto.ExecuteResponse, error) {
	resp := <-s.rows
	log.Printf("[WARN] LocalPluginStream Recv %v", resp)
	return resp, nil
}

func (s *LocalPluginStream) Context() context.Context {
	return s.ctx
}

package shared

import (
	"context"

	pb "github.com/turbotio/steampipe-plugin-sdk/grpc/proto"
)

// GRPCClient is an implementation of
//WrapperPluginClient service that talks over RPC.
type GRPCClient struct {
	// Proto client use to make the grpc service calls.
	client pb.WrapperPluginClient
	// this context is created by the plugin package, and is canceled when the
	// plugin process ends.
	ctx context.Context
}

func (c *GRPCClient) GetSchema(req *pb.GetSchemaRequest) (*pb.GetSchemaResponse, error) {
	return c.client.GetSchema(c.ctx, req)
}

func (c *GRPCClient) Execute(req *pb.ExecuteRequest) (pb.WrapperPlugin_ExecuteClient, error) {
	return c.client.Execute(c.ctx, req)
}

// Here is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
	// This is the real implementation
	Impl WrapperPluginServer
}

func (m *GRPCServer) GetSchema(_ context.Context, req *pb.GetSchemaRequest) (*pb.GetSchemaResponse, error) {
	return m.Impl.GetSchema(req)
}

func (m *GRPCServer) Execute(req *pb.ExecuteRequest, server pb.WrapperPlugin_ExecuteServer) error {
	return m.Impl.Execute(req, server)
}

package shared

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

// GRPCClient is an implementation of
//WrapperPluginClient service that talks over RPC.
type GRPCClient struct {
	// Proto client use to make the grpc service calls.
	client proto.WrapperPluginClient
	// this context is created by the plugin package, and is canceled when the
	// plugin process ends.
	ctx context.Context
}

func (c *GRPCClient) GetSchema(req *proto.GetSchemaRequest) (*proto.GetSchemaResponse, error) {
	return c.client.GetSchema(c.ctx, req)
}

func (c *GRPCClient) Execute(req *proto.ExecuteRequest) (proto.WrapperPlugin_ExecuteClient, error) {
	return c.client.Execute(c.ctx, req)
}

func (c *GRPCClient) SetConnectionConfig(req *proto.SetConnectionConfigRequest) (*proto.SetConnectionConfigResponse, error) {
	return c.client.SetConnectionConfig(c.ctx, req)
}

// Here is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
	// This is the real implementation
	Impl WrapperPluginServer
}

func (m *GRPCServer) GetSchema(_ context.Context, req *proto.GetSchemaRequest) (*proto.GetSchemaResponse, error) {
	return m.Impl.GetSchema(req)
}

func (m *GRPCServer) Execute(req *proto.ExecuteRequest, server proto.WrapperPlugin_ExecuteServer) error {
	return m.Impl.Execute(req, server)

}

func (m *GRPCServer) SetConnectionConfig(_ context.Context, req *proto.SetConnectionConfigRequest) (*proto.SetConnectionConfigResponse, error) {
	return m.Impl.SetConnectionConfig(req)
}

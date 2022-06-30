package shared

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"google.golang.org/grpc"
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

func (c *GRPCClient) Execute(req *proto.ExecuteRequest) (proto.WrapperPlugin_ExecuteClient, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(c.ctx)
	client, err := c.client.Execute(ctx, req)
	return client, ctx, cancel, err
}

func (c *GRPCClient) SetConnectionConfig(req *proto.SetConnectionConfigRequest) (*proto.SetConnectionConfigResponse, error) {
	return c.client.SetConnectionConfig(c.ctx, req)
}

func (c *GRPCClient) GetSupportedOperations(req *proto.GetSupportedOperationsRequest) (*proto.GetSupportedOperationsResponse, error) {
	return c.client.GetSupportedOperations(c.ctx, req)
}

func (c *GRPCClient) EstablishCacheConnection() (proto.WrapperPlugin_EstablishCacheConnectionClient, error) {
	// TODO failed attempt to increase the max message size for cache stream
	return c.client.EstablishCacheConnection(c.ctx, grpc.MaxCallSendMsgSize(64*1024*1024), grpc.MaxCallRecvMsgSize(64*1024*1024))
	//return c.client.EstablishCacheConnection(c.ctx)
}

// GRPCServer is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
	proto.UnimplementedWrapperPluginServer
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

func (m *GRPCServer) GetSupportedOperations(_ context.Context, req *proto.GetSupportedOperationsRequest) (*proto.GetSupportedOperationsResponse, error) {
	return m.Impl.GetSupportedOperations(req)
}

func (m *GRPCServer) EstablishCacheConnection(server proto.WrapperPlugin_EstablishCacheConnectionServer) error {
	return m.Impl.EstablishCacheConnection(server)
}

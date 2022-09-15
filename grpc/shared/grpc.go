package shared

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
)

// GRPCClient is an implementation of
// WrapperPluginClient service that talks over RPC.
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

func (c *GRPCClient) SetAllConnectionConfigs(req *proto.SetAllConnectionConfigsRequest) (*proto.SetConnectionConfigResponse, error) {
	return c.client.SetAllConnectionConfigs(c.ctx, req)
}

func (c *GRPCClient) UpdateConnectionConfigs(req *proto.UpdateConnectionConfigsRequest) (*proto.UpdateConnectionConfigsResponse, error) {
	return c.client.UpdateConnectionConfigs(c.ctx, req)
}

func (c *GRPCClient) GetSupportedOperations(req *proto.GetSupportedOperationsRequest) (*proto.GetSupportedOperationsResponse, error) {
	return c.client.GetSupportedOperations(c.ctx, req)
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

func (m *GRPCServer) SetAllConnectionConfigs(_ context.Context, req *proto.SetAllConnectionConfigsRequest) (*proto.SetConnectionConfigResponse, error) {
	return m.Impl.SetAllConnectionConfigs(req)
}

func (m *GRPCServer) UpdateConnectionConfigs(_ context.Context, req *proto.UpdateConnectionConfigsRequest) (*proto.UpdateConnectionConfigsResponse, error) {
	return m.Impl.UpdateConnectionConfigs(req)
}

func (m *GRPCServer) GetSupportedOperations(_ context.Context, req *proto.GetSupportedOperationsRequest) (*proto.GetSupportedOperationsResponse, error) {
	return m.Impl.GetSupportedOperations(req)
}

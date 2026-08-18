package shared

import (
	"context"
	"os"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
)

// defaultAdminRPCTimeout bounds the administrative unary RPCs made to the
// plugin (schema fetch, connection-config updates, cache and rate-limiter
// options). These calls were previously issued on the long-lived plugin
// context with no deadline, so a plugin that never answered blocked the
// caller forever. In Steampipe that permanently wedges RefreshConnections
// (which is single-flight with a single queued slot), silently disabling all
// future connection refreshes until the process is restarted.
//
// The default is deliberately generous — well above the default plugin start
// timeout (240s) — so legitimately slow operations (e.g. SetAllConnectionConfigs
// for hundreds of connections) are unaffected; the goal is only to convert
// "blocked forever" into an error the caller can surface and retry.
const defaultAdminRPCTimeout = 5 * time.Minute

// adminRPCTimeout is the effective deadline for administrative unary RPCs,
// overridable via the STEAMPIPE_ADMIN_RPC_TIMEOUT environment variable
// (a Go duration string, e.g. "90s" or "10m").
var adminRPCTimeout = func() time.Duration {
	if v := os.Getenv("STEAMPIPE_ADMIN_RPC_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return defaultAdminRPCTimeout
}()

// GRPCClient is an implementation of
// WrapperPluginClient service that talks over RPC.
type GRPCClient struct {
	// Proto client use to make the grpc service calls.
	client proto.WrapperPluginClient
	// this context is created by the plugin package, and is canceled when the
	// plugin process ends.
	ctx context.Context
}

// adminContext derives a deadline-bounded context for administrative unary
// RPCs. Streaming RPCs (Execute, EstablishMessageStream) are long-lived by
// design and intentionally do NOT use this.
func (c *GRPCClient) adminContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.ctx, adminRPCTimeout)
}

func (c *GRPCClient) EstablishMessageStream() (proto.WrapperPlugin_EstablishMessageStreamClient, error) {
	return c.client.EstablishMessageStream(c.ctx, &proto.EstablishMessageStreamRequest{})
}

func (c *GRPCClient) GetSchema(req *proto.GetSchemaRequest) (*proto.GetSchemaResponse, error) {
	ctx, cancel := c.adminContext()
	defer cancel()
	return c.client.GetSchema(ctx, req)
}

func (c *GRPCClient) Execute(req *proto.ExecuteRequest) (proto.WrapperPlugin_ExecuteClient, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(c.ctx)
	client, err := c.client.Execute(ctx, req)
	return client, ctx, cancel, err
}

func (c *GRPCClient) SetConnectionConfig(req *proto.SetConnectionConfigRequest) (*proto.SetConnectionConfigResponse, error) {
	ctx, cancel := c.adminContext()
	defer cancel()
	return c.client.SetConnectionConfig(ctx, req)
}

func (c *GRPCClient) SetAllConnectionConfigs(req *proto.SetAllConnectionConfigsRequest) (*proto.SetConnectionConfigResponse, error) {
	ctx, cancel := c.adminContext()
	defer cancel()
	return c.client.SetAllConnectionConfigs(ctx, req)
}

func (c *GRPCClient) UpdateConnectionConfigs(req *proto.UpdateConnectionConfigsRequest) (*proto.UpdateConnectionConfigsResponse, error) {
	ctx, cancel := c.adminContext()
	defer cancel()
	return c.client.UpdateConnectionConfigs(ctx, req)
}

func (c *GRPCClient) GetSupportedOperations(req *proto.GetSupportedOperationsRequest) (*proto.GetSupportedOperationsResponse, error) {
	ctx, cancel := c.adminContext()
	defer cancel()
	return c.client.GetSupportedOperations(ctx, req)
}

func (c *GRPCClient) SetCacheOptions(req *proto.SetCacheOptionsRequest) (*proto.SetCacheOptionsResponse, error) {
	ctx, cancel := c.adminContext()
	defer cancel()
	return c.client.SetCacheOptions(ctx, req)
}

func (c *GRPCClient) SetConnectionCacheOptions(req *proto.SetConnectionCacheOptionsRequest) (*proto.SetConnectionCacheOptionsResponse, error) {
	ctx, cancel := c.adminContext()
	defer cancel()
	return c.client.SetConnectionCacheOptions(ctx, req)
}

func (c *GRPCClient) SetRateLimiters(req *proto.SetRateLimitersRequest) (*proto.SetRateLimitersResponse, error) {
	ctx, cancel := c.adminContext()
	defer cancel()
	return c.client.SetRateLimiters(ctx, req)
}

func (c *GRPCClient) GetRateLimiters(req *proto.GetRateLimitersRequest) (*proto.GetRateLimitersResponse, error) {
	ctx, cancel := c.adminContext()
	defer cancel()
	return c.client.GetRateLimiters(ctx, req)
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

func (m *GRPCServer) SetCacheOptions(_ context.Context, req *proto.SetCacheOptionsRequest) (*proto.SetCacheOptionsResponse, error) {
	return m.Impl.SetCacheOptions(req)
}

func (m *GRPCServer) SetConnectionCacheOptions(_ context.Context, req *proto.SetConnectionCacheOptionsRequest) (*proto.SetConnectionCacheOptionsResponse, error) {
	return m.Impl.SetConnectionCacheOptions(req)
}

func (m *GRPCServer) SetRateLimiters(_ context.Context, req *proto.SetRateLimitersRequest) (*proto.SetRateLimitersResponse, error) {
	return m.Impl.SetRateLimiters(req)
}
func (m *GRPCServer) GetRateLimiters(_ context.Context, req *proto.GetRateLimitersRequest) (*proto.GetRateLimitersResponse, error) {
	return m.Impl.GetRateLimiters(req)
}

func (m *GRPCServer) EstablishMessageStream(_ *proto.EstablishMessageStreamRequest, server proto.WrapperPlugin_EstablishMessageStreamServer) error {
	return m.Impl.EstablishMessageStream(server)
}

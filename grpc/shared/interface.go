// Package shared contains shared data between the host and plugins.
package shared

import (
	"context"

	pb "github.com/turbot/steampipe-plugin-sdk/grpc/proto"

	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
)

// Handshake is a common handshake that is shared by plugin and host.
// See https://github.com/hashicorp/terraform/blob/ba6e243bd97fda935f903da0d420e5ed94e35c9e/plugin/serve.go#L24
var Handshake = plugin.HandshakeConfig{
	MagicCookieKey:   "MANTIX_PLUGIN_MAGIC_COOKIE",
	MagicCookieValue: "really-complex-permanent-string-value",
}

// this is the interface for the plugin service
// NOTE there are 2 versions
// - the server interface (implemented by the actual plugin)
// - the client interface (implemented by the client stub)
// this is because the signature of the client and server are difference when using grpc streaming
type WrapperPluginServer interface {
	GetSchema(req *pb.GetSchemaRequest) (*pb.GetSchemaResponse, error)
	Execute(req *pb.ExecuteRequest, stream pb.WrapperPlugin_ExecuteServer) error
	SetConnectionConfig(req *pb.SetConnectionConfigRequest) (*pb.SetConnectionConfigResponse, error)
}

type WrapperPluginClient interface {
	GetSchema(request *pb.GetSchemaRequest) (*pb.GetSchemaResponse, error)
	Execute(req *pb.ExecuteRequest) (pb.WrapperPlugin_ExecuteClient, error)
	SetConnectionConfig(req *pb.SetConnectionConfigRequest) (*pb.SetConnectionConfigResponse, error)
}

// This is the implementation of plugin.GRPCServer so we can serve/consume this.
type WrapperPlugin struct {
	// GRPCPlugin must still implement the Stub interface
	plugin.Plugin
	// Concrete implementation, written in Go. This is only used for plugins
	// that are written in Go.
	Impl WrapperPluginServer
}

func (p *WrapperPlugin) GRPCServer(_ *plugin.GRPCBroker, s *grpc.Server) error {
	pb.RegisterWrapperPluginServer(s, &GRPCServer{Impl: p.Impl})
	return nil
}

// return a GRPCClient, called by Dispense
func (p *WrapperPlugin) GRPCClient(ctx context.Context, _ *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{client: pb.NewWrapperPluginClient(c), ctx: ctx}, nil
}

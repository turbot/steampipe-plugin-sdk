package grpc

import (
	"github.com/hashicorp/go-plugin"
	pb "github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	pluginshared "github.com/turbot/steampipe-plugin-sdk/grpc/shared"
	"github.com/turbot/steampipe-plugin-sdk/version"
)

type ExecuteFunc func(req *pb.ExecuteRequest, stream pb.WrapperPlugin_ExecuteServer) error
type GetSchemaFunc func() (map[string]*pb.TableSchema, error)

// PluginServer :: server for a single plugin
type PluginServer struct {
	pb.UnimplementedWrapperPluginServer
	pluginName    string
	executeFunc   ExecuteFunc
	getSchemaFunc GetSchemaFunc
}

func NewPluginServer(pluginName string, getSchemaFunc GetSchemaFunc, executeFunc ExecuteFunc) *PluginServer {

	return &PluginServer{
		pluginName:    pluginName,
		executeFunc:   executeFunc,
		getSchemaFunc: getSchemaFunc,
	}
}

func (s PluginServer) GetSchema(_ *pb.GetSchemaRequest) (*pb.GetSchemaResponse, error) {
	schema, err := s.getSchemaFunc()
	return &pb.GetSchemaResponse{Schema: &pb.Schema{Schema: schema, SdkVersion: version.String()}}, err
}

func (s PluginServer) Execute(req *pb.ExecuteRequest, stream pb.WrapperPlugin_ExecuteServer) error {
	return s.executeFunc(req, stream)
}

func (s PluginServer) Serve() {
	pluginMap := map[string]plugin.Plugin{
		s.pluginName: &pluginshared.WrapperPlugin{Impl: s},
	}

	plugin.Serve(&plugin.ServeConfig{
		Plugins:    pluginMap,
		GRPCServer: plugin.DefaultGRPCServer,
		// A non-nil value here enables gRPC serving for this plugin...
		HandshakeConfig: pluginshared.Handshake,
	})
}

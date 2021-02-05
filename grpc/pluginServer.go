package grpc

import (
	"github.com/hashicorp/go-plugin"
	pb "github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	pluginshared "github.com/turbot/steampipe-plugin-sdk/grpc/shared"
	"github.com/turbot/steampipe-plugin-sdk/version"
)

type ExecuteFunc func(req *pb.ExecuteRequest, stream pb.WrapperPlugin_ExecuteServer) error
type GetSchemaFunc func() (map[string]*pb.TableSchema, error)
type SetConnectionConfigFunc func(string, string) error

// PluginServer :: server for a single plugin
type PluginServer struct {
	pb.UnimplementedWrapperPluginServer
	pluginName              string
	executeFunc             ExecuteFunc
	setConnectionConfigFunc SetConnectionConfigFunc
	getSchemaFunc           GetSchemaFunc
}

func NewPluginServer(pluginName string, getSchemaFunc GetSchemaFunc, executeFunc ExecuteFunc, setConnectionConfigFunc SetConnectionConfigFunc) *PluginServer {
	return &PluginServer{
		pluginName:              pluginName,
		executeFunc:             executeFunc,
		setConnectionConfigFunc: setConnectionConfigFunc,
		getSchemaFunc:           getSchemaFunc,
	}
}

func (s PluginServer) GetSchema(_ *pb.GetSchemaRequest) (*pb.GetSchemaResponse, error) {
	schema, err := s.getSchemaFunc()
	return &pb.GetSchemaResponse{Schema: &pb.Schema{Schema: schema, SdkVersion: version.String()}}, err
}

func (s PluginServer) Execute(req *pb.ExecuteRequest, stream pb.WrapperPlugin_ExecuteServer) error {
	return s.executeFunc(req, stream)
}

func (s PluginServer) SetConnectionConfig(req *pb.SetConnectionConfigRequest) (*pb.SetConnectionConfigResponse, error) {
	err := s.setConnectionConfigFunc(req.ConnectionName, req.ConnectionConfig)
	return &pb.SetConnectionConfigResponse{}, err
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

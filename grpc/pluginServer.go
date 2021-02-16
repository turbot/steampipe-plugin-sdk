package grpc

import (
	"github.com/turbot/go-kit/helpers"

	"github.com/hashicorp/go-plugin"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	pluginshared "github.com/turbot/steampipe-plugin-sdk/grpc/shared"
	"github.com/turbot/steampipe-plugin-sdk/version"
)

type ExecuteFunc func(req *proto.ExecuteRequest, stream proto.WrapperPlugin_ExecuteServer) error
type GetSchemaFunc func() (map[string]*proto.TableSchema, error)
type SetConnectionConfigFunc func(string, string) error

// PluginServer :: server for a single plugin
type PluginServer struct {
	proto.UnimplementedWrapperPluginServer
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

func (s PluginServer) GetSchema(_ *proto.GetSchemaRequest) (res *proto.GetSchemaResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()
	schema, err := s.getSchemaFunc()
	return &proto.GetSchemaResponse{Schema: &proto.Schema{Schema: schema, SdkVersion: version.String()}}, err
}

func (s PluginServer) Execute(req *proto.ExecuteRequest, stream proto.WrapperPlugin_ExecuteServer) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()
	return s.executeFunc(req, stream)
}

func (s PluginServer) SetConnectionConfig(req *proto.SetConnectionConfigRequest) (res *proto.SetConnectionConfigResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()
	err = s.setConnectionConfigFunc(req.ConnectionName, req.ConnectionConfig)
	return &proto.SetConnectionConfigResponse{}, err
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

package grpc

import (
	"github.com/hashicorp/go-plugin"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	pluginshared "github.com/turbot/steampipe-plugin-sdk/v3/grpc/shared"
	"github.com/turbot/steampipe-plugin-sdk/v3/version"
	"log"
)

type PluginSchema struct {
	Schema map[string]*proto.TableSchema
	Mode   string
}
type ExecuteFunc func(req *proto.ExecuteRequest, stream proto.WrapperPlugin_ExecuteServer) error
type GetSchemaFunc func(string) (*PluginSchema, error)
type SetConnectionConfigFunc func(string, string) error
type SetAllConnectionConfigsFunc func([]*proto.ConnectionConfig) error
type EstablishCacheConnectionFunc func(stream proto.WrapperPlugin_EstablishCacheConnectionServer) error

// PluginServer is the server for a single plugin
type PluginServer struct {
	proto.UnimplementedWrapperPluginServer
	pluginName                   string
	executeFunc                  ExecuteFunc
	setConnectionConfigFunc      SetConnectionConfigFunc
	setAllConnectionConfigsFunc  SetAllConnectionConfigsFunc
	getSchemaFunc                GetSchemaFunc
	establishCacheConnectionFunc EstablishCacheConnectionFunc
}

func NewPluginServer(pluginName string, setConnectionConfigFunc SetConnectionConfigFunc, setAllConnectionConfigsFunc SetAllConnectionConfigsFunc, getSchemaFunc GetSchemaFunc, executeFunc ExecuteFunc, establishCacheConnectionFunc EstablishCacheConnectionFunc) *PluginServer {
	return &PluginServer{
		pluginName:                   pluginName,
		executeFunc:                  executeFunc,
		setConnectionConfigFunc:      setConnectionConfigFunc,
		setAllConnectionConfigsFunc:  setAllConnectionConfigsFunc,
		getSchemaFunc:                getSchemaFunc,
		establishCacheConnectionFunc: establishCacheConnectionFunc,
	}
}

func (s PluginServer) GetSchema(req *proto.GetSchemaRequest) (res *proto.GetSchemaResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()
	schema, err := s.getSchemaFunc(req.Connection)
	if err != nil {
		return nil, err
	}

	return &proto.GetSchemaResponse{
		Schema: &proto.Schema{
			Schema:          schema.Schema,
			Mode:            schema.Mode,
			SdkVersion:      version.String(),
			ProtocolVersion: version.ProtocolVersion,
		},
	}, err
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

func (s PluginServer) SetAllConnectionConfigs(req *proto.SetAllConnectionConfigsRequest) (res *proto.SetConnectionConfigResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()
	err = s.setAllConnectionConfigsFunc(req.Configs)
	return &proto.SetConnectionConfigResponse{}, err
}

func (s PluginServer) GetSupportedOperations(*proto.GetSupportedOperationsRequest) (*proto.GetSupportedOperationsResponse, error) {
	return &proto.GetSupportedOperationsResponse{
		QueryCache:          true,
		CacheStream:         true,
		MultipleConnections: true,
	}, nil
}

func (s PluginServer) EstablishCacheConnection(stream proto.WrapperPlugin_EstablishCacheConnectionServer) error {
	return s.establishCacheConnectionFunc(stream)
}

func (s PluginServer) Serve() {
	pluginMap := map[string]plugin.Plugin{
		s.pluginName: &pluginshared.WrapperPlugin{Impl: s},
	}
	log.Printf("[WARN] SERVE!!!!!!!!!!!!!! ")

	plugin.Serve(&plugin.ServeConfig{
		Plugins:    pluginMap,
		GRPCServer: plugin.DefaultGRPCServer,
		// A non-nil value here enables gRPC serving for this plugin...
		HandshakeConfig: pluginshared.Handshake,
	})
}

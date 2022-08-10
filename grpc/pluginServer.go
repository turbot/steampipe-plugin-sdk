package grpc

import (
	"fmt"
	"github.com/hashicorp/go-plugin"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	pluginshared "github.com/turbot/steampipe-plugin-sdk/v4/grpc/shared"
	"github.com/turbot/steampipe-plugin-sdk/v4/version"
)

type PluginSchema struct {
	Schema map[string]*proto.TableSchema
	Mode   string
}
type ExecuteFunc func(req *proto.ExecuteRequest, stream proto.WrapperPlugin_ExecuteServer) error
type GetSchemaFunc func(string) (*PluginSchema, error)
type SetConnectionConfigFunc func(string, string) error
type SetAllConnectionConfigsFunc func([]*proto.ConnectionConfig, int) error
type UpdateConnectionConfigsFunc func([]*proto.ConnectionConfig, []*proto.ConnectionConfig, []*proto.ConnectionConfig) error

// PluginServer is the server for a single plugin
type PluginServer struct {
	proto.UnimplementedWrapperPluginServer
	pluginName                  string
	executeFunc                 ExecuteFunc
	setConnectionConfigFunc     SetConnectionConfigFunc
	setAllConnectionConfigsFunc SetAllConnectionConfigsFunc
	updateConnectionConfigsFunc UpdateConnectionConfigsFunc
	getSchemaFunc               GetSchemaFunc
}

func NewPluginServer(pluginName string,
	setConnectionConfigFunc SetConnectionConfigFunc,
	setAllConnectionConfigsFunc SetAllConnectionConfigsFunc,
	updateConnectionConfigsFunc UpdateConnectionConfigsFunc,
	getSchemaFunc GetSchemaFunc,
	executeFunc ExecuteFunc) *PluginServer {

	return &PluginServer{
		pluginName:                  pluginName,
		executeFunc:                 executeFunc,
		setConnectionConfigFunc:     setConnectionConfigFunc,
		setAllConnectionConfigsFunc: setAllConnectionConfigsFunc,
		updateConnectionConfigsFunc: updateConnectionConfigsFunc,
		getSchemaFunc:               getSchemaFunc,
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

	// NOTE: Compatibility
	// if a pre-v16 version of Steampipe is being used,
	// the deprecated Connection, CacheEnabled and CacheTtl will be set, and ExecuteConnectionData will be nil
	// populate ExecuteConnectionData
	if req.ExecuteConnectionData == nil {
		if req.Connection == "" {
			return fmt.Errorf("either ExecuteConnectionData or Connection must be provided")
		}
		req.ExecuteConnectionData = map[string]*proto.ExecuteConnectionData{
			req.Connection: {
				Limit:        req.QueryContext.Limit,
				CacheEnabled: req.CacheEnabled,
				CacheTtl:     req.CacheTtl,
			},
		}
	}

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
	err = s.setAllConnectionConfigsFunc(req.Configs, int(req.MaxCacheSizeMb))
	return &proto.SetConnectionConfigResponse{}, err
}

func (s PluginServer) UpdateConnectionConfigs(req *proto.UpdateConnectionConfigsRequest) (res *proto.UpdateConnectionConfigsResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()
	err = s.updateConnectionConfigsFunc(req.Added, req.Deleted, req.Changed)
	return &proto.UpdateConnectionConfigsResponse{}, err
}

func (s PluginServer) GetSupportedOperations(*proto.GetSupportedOperationsRequest) (*proto.GetSupportedOperationsResponse, error) {
	return &proto.GetSupportedOperationsResponse{
		QueryCache:          true,
		MultipleConnections: true,
	}, nil
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

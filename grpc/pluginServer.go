package grpc

import (
	"fmt"

	"github.com/hashicorp/go-plugin"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/anywhere"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	pluginshared "github.com/turbot/steampipe-plugin-sdk/v5/grpc/shared"
	"github.com/turbot/steampipe-plugin-sdk/v5/row_stream"
	"github.com/turbot/steampipe-plugin-sdk/v5/version"
)

type ExecuteFunc func(req *proto.ExecuteRequest, stream row_stream.Sender) error
type GetSchemaFunc func(string) (*PluginSchema, error)
type SetConnectionConfigFunc func(string, string) error
type SetAllConnectionConfigsFunc func([]*proto.ConnectionConfig, int) (map[string]error, error)
type UpdateConnectionConfigsFunc func([]*proto.ConnectionConfig, []*proto.ConnectionConfig, []*proto.ConnectionConfig) (map[string]error, error)
type SetCacheOptionsFunc func(*proto.SetCacheOptionsRequest) error
type SetConnectionCacheOptionsFunc func(*proto.SetConnectionCacheOptionsRequest) error
type SetRateLimitersFunc func(*proto.SetRateLimitersRequest) error
type GetRateLimitersFunc func() []*proto.RateLimiterDefinition
type EstablishMessageStreamFunc func(stream proto.WrapperPlugin_EstablishMessageStreamServer) error
type GetSchemaModeFunc func() string

// PluginServer is the server for a single plugin
type PluginServer struct {
	proto.UnimplementedWrapperPluginServer
	pluginName                    string
	executeFunc                   ExecuteFunc
	setConnectionConfigFunc       SetConnectionConfigFunc
	setAllConnectionConfigsFunc   SetAllConnectionConfigsFunc
	updateConnectionConfigsFunc   UpdateConnectionConfigsFunc
	getSchemaFunc                 GetSchemaFunc
	establishMessageStreamFunc    EstablishMessageStreamFunc
	setCacheOptionsFunc           SetCacheOptionsFunc
	setConnectionCacheOptionsFunc SetConnectionCacheOptionsFunc
	setRateLimitersFunc           SetRateLimitersFunc
	getRateLimitersFunc           GetRateLimitersFunc
	getSchemaModeFunc             GetSchemaModeFunc
}

func NewPluginServer(pluginName string,
	setConnectionConfigFunc SetConnectionConfigFunc,
	setAllConnectionConfigsFunc SetAllConnectionConfigsFunc,
	updateConnectionConfigsFunc UpdateConnectionConfigsFunc,
	getSchemaFunc GetSchemaFunc,
	executeFunc ExecuteFunc,
	establishMessageStreamFunc EstablishMessageStreamFunc,
	setCacheOptionsFunc SetCacheOptionsFunc,
	setRateLimitersFunc SetRateLimitersFunc,
	getRateLimitersFunc GetRateLimitersFunc,
	setConnectionCacheOptionsFunc SetConnectionCacheOptionsFunc,
	GetSchemaModeFunc GetSchemaModeFunc,
) *PluginServer {

	return &PluginServer{
		pluginName:                    pluginName,
		executeFunc:                   executeFunc,
		setConnectionConfigFunc:       setConnectionConfigFunc,
		setAllConnectionConfigsFunc:   setAllConnectionConfigsFunc,
		updateConnectionConfigsFunc:   updateConnectionConfigsFunc,
		getSchemaFunc:                 getSchemaFunc,
		establishMessageStreamFunc:    establishMessageStreamFunc,
		setCacheOptionsFunc:           setCacheOptionsFunc,
		setRateLimitersFunc:           setRateLimitersFunc,
		getRateLimitersFunc:           getRateLimitersFunc,
		setConnectionCacheOptionsFunc: setConnectionCacheOptionsFunc,
		getSchemaModeFunc:             GetSchemaModeFunc,
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

// Execute implements the WrapperPluginServer interface and is used to execute calls vis GRPC
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

// CallExecuteAsync directly calls the execute function and is used to execute in-process
func (s PluginServer) CallExecuteAsync(req *proto.ExecuteRequest, stream *anywhere.LocalPluginStream) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				stream.Error(helpers.ToError(r))
			}
		}()

		err := s.executeFunc(req, stream)
		if err != nil {
			stream.Error(err)
			return
		}
		// Signal completion by sending nil
		stream.Send(nil)
	}()
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
	failedConnections, err := s.setAllConnectionConfigsFunc(req.Configs, int(req.MaxCacheSizeMb))
	res = &proto.SetConnectionConfigResponse{
		FailedConnections: make(map[string]string),
	}
	for connectionName, err := range failedConnections {
		res.FailedConnections[connectionName] = err.Error()
	}
	return
}

func (s PluginServer) UpdateConnectionConfigs(req *proto.UpdateConnectionConfigsRequest) (res *proto.UpdateConnectionConfigsResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()
	failedConnections, err := s.updateConnectionConfigsFunc(req.Added, req.Deleted, req.Changed)
	res = &proto.UpdateConnectionConfigsResponse{
		FailedConnections: make(map[string]string),
	}
	for connectionName, err := range failedConnections {
		res.FailedConnections[connectionName] = err.Error()
	}
	return &proto.UpdateConnectionConfigsResponse{}, err
}

func (s PluginServer) GetSupportedOperations(*proto.GetSupportedOperationsRequest) (*proto.GetSupportedOperationsResponse, error) {
	return &proto.GetSupportedOperationsResponse{
		QueryCache:          true,
		MultipleConnections: true,
		MessageStream:       true,
		SetCacheOptions:     true,
		RateLimiters:        true,
	}, nil
}

func (s PluginServer) SetCacheOptions(req *proto.SetCacheOptionsRequest) (*proto.SetCacheOptionsResponse, error) {
	err := s.setCacheOptionsFunc(req)
	return &proto.SetCacheOptionsResponse{}, err
}

func (s PluginServer) SetConnectionCacheOptions(req *proto.SetConnectionCacheOptionsRequest) (*proto.SetConnectionCacheOptionsResponse, error) {
	err := s.setConnectionCacheOptionsFunc(req)
	return &proto.SetConnectionCacheOptionsResponse{}, err
}

func (s PluginServer) SetRateLimiters(req *proto.SetRateLimitersRequest) (*proto.SetRateLimitersResponse, error) {
	err := s.setRateLimitersFunc(req)
	return &proto.SetRateLimitersResponse{}, err
}

func (s PluginServer) GetRateLimiters(*proto.GetRateLimitersRequest) (*proto.GetRateLimitersResponse, error) {
	rateLimiters := s.getRateLimitersFunc()
	return &proto.GetRateLimitersResponse{Definitions: rateLimiters}, nil
}

func (s PluginServer) EstablishMessageStream(stream proto.WrapperPlugin_EstablishMessageStreamServer) error {
	return s.establishMessageStreamFunc(stream)
}

func (s PluginServer) GetSchemaMode() string {
	return s.getSchemaModeFunc()
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

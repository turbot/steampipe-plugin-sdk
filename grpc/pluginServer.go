package grpc

import (
	"log"
	"sync"
	"time"

	"github.com/turbot/go-kit/helpers"

	"github.com/hashicorp/go-plugin"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	pluginshared "github.com/turbot/steampipe-plugin-sdk/grpc/shared"
	"github.com/turbot/steampipe-plugin-sdk/version"
)

type PluginSchema struct {
	Schema map[string]*proto.TableSchema
	Mode   string
}
type ExecuteFunc func(req *proto.ExecuteRequest, stream proto.WrapperPlugin_ExecuteServer) error
type GetSchemaFunc func() (*PluginSchema, error)
type SetConnectionConfigFunc func(string, string) error

// PluginServer is the server for a single plugin
type PluginServer struct {
	proto.UnimplementedWrapperPluginServer
	pluginName              string
	executeFunc             ExecuteFunc
	setConnectionConfigFunc SetConnectionConfigFunc
	getSchemaFunc           GetSchemaFunc
	timingLock              sync.Mutex
	lastScanTime            time.Time
}

func NewPluginServer(pluginName string, setConnectionConfigFunc SetConnectionConfigFunc, getSchemaFunc GetSchemaFunc, executeFunc ExecuteFunc) *PluginServer {
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
	s.throttle()
	return s.executeFunc(req, stream)
}

func (s PluginServer) throttle() {
	minScanInterval := 50 * time.Millisecond
	s.timingLock.Lock()
	defer s.timingLock.Unlock()
	timeSince := time.Since(s.lastScanTime)
	if timeSince < minScanInterval {

		sleepTime := minScanInterval - timeSince
		log.Printf("[WARN] *********  PluginServer Execute timeSince %dms, sleeping %dms", timeSince.Milliseconds(), sleepTime.Milliseconds())
		time.Sleep(sleepTime)
	}
	s.lastScanTime = time.Now()
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

func (s PluginServer) GetSupportedOperations(*proto.GetSupportedOperationsRequest) (*proto.GetSupportedOperationsResponse, error) {
	return &proto.GetSupportedOperationsResponse{
		QueryCache: true,
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

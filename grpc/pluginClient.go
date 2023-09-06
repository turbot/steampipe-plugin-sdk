package grpc

import (
	"context"
	pluginshared "github.com/turbot/steampipe-plugin-sdk/v5/grpc/shared"
	"io/ioutil"
	"log"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
)

// PluginClient is the client object used by clients of the plugin
type PluginClient struct {
	Name   string
	Stub   pluginshared.WrapperPluginClient
	client *plugin.Client
}

func NewPluginClient(client *plugin.Client, pluginName string) (*PluginClient, error) {
	log.Printf("[TRACE] NewPluginClient for plugin %s", pluginName)

	// connect via RPC
	rpcClient, err := client.Client()
	if err != nil {
		return nil, err
	}

	// request the plugin
	raw, err := rpcClient.Dispense(pluginName)
	if err != nil {
		return nil, err
	}
	// we should have a stub plugin now
	p := raw.(pluginshared.WrapperPluginClient)
	res := &PluginClient{
		Name:   pluginName,
		client: client,
		Stub:   p,
	}
	return res, nil
}

func NewPluginClientFromReattach(reattach *plugin.ReattachConfig, pluginName string) (*PluginClient, error) {
	log.Printf("[TRACE] NewPluginClientFromReattach for plugin %s", pluginName)
	// create the plugin map
	pluginMap := map[string]plugin.Plugin{
		pluginName: &pluginshared.WrapperPlugin{},
	}
	// discard logging from the client (plugin logs will still flow through to the log file as the plugin manager set this up)
	logger := logging.NewLogger(&hclog.LoggerOptions{Name: "plugin", Output: ioutil.Discard})

	// create grpc client
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig:  pluginshared.Handshake,
		Plugins:          pluginMap,
		Reattach:         reattach,
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		Logger:           logger,
	})
	res, err := NewPluginClient(client, pluginName)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (c *PluginClient) Execute(req *proto.ExecuteRequest) (str proto.WrapperPlugin_ExecuteClient, ctx context.Context, cancel context.CancelFunc, err error) {
	return c.Stub.Execute(req)
}

func (c *PluginClient) SetConnectionConfig(req *proto.SetConnectionConfigRequest) error {
	_, err := c.Stub.SetConnectionConfig(req)
	if err != nil {
		// create a new cleaner error, ignoring Not Implemented errors for backwards compatibility
		return HandleGrpcError(err, c.Name, "SetConnectionConfig")
	}
	return nil
}

func (c *PluginClient) SetAllConnectionConfigs(req *proto.SetAllConnectionConfigsRequest) (*proto.SetConnectionConfigResponse, error) {
	resp, err := c.Stub.SetAllConnectionConfigs(req)
	if err != nil {
		// create a new cleaner error, ignoring Not Implemented errors for backwards compatibility
		return nil, HandleGrpcError(err, c.Name, "SetAllConnectionConfigs")
	}
	return resp, nil
}

func (c *PluginClient) UpdateConnectionConfigs(req *proto.UpdateConnectionConfigsRequest) error {
	_, err := c.Stub.UpdateConnectionConfigs(req)
	if err != nil {
		// create a new cleaner error, ignoring Not Implemented errors for backwards compatibility
		return HandleGrpcError(err, c.Name, "UpdateConnectionConfigs")
	}
	return nil
}

func (c *PluginClient) GetSupportedOperations() (*proto.GetSupportedOperationsResponse, error) {
	resp, err := c.Stub.GetSupportedOperations(&proto.GetSupportedOperationsRequest{})
	if err != nil {
		return nil, HandleGrpcError(err, c.Name, "GetSupportedOperations")
	}
	return resp, nil
}

func (c *PluginClient) SetCacheOptions(req *proto.SetCacheOptionsRequest) (*proto.SetCacheOptionsResponse, error) {
	resp, err := c.Stub.SetCacheOptions(req)
	if err != nil {
		return nil, HandleGrpcError(err, c.Name, "SetCacheOptions")
	}
	return resp, nil
}

func (c *PluginClient) SetRateLimiters(req *proto.SetRateLimitersRequest) (*proto.SetRateLimitersResponse, error) {
	resp, err := c.Stub.SetRateLimiters(req)
	if err != nil {
		return nil, HandleGrpcError(err, c.Name, "SetRateLimiters")
	}
	return resp, nil
}

func (c *PluginClient) GetRateLimiters(req *proto.GetRateLimitersRequest) (*proto.GetRateLimitersResponse, error) {
	resp, err := c.Stub.GetRateLimiters(req)
	if err != nil {
		return nil, HandleGrpcError(err, c.Name, "GetRateLimiters")
	}
	return resp, nil
}

func (c *PluginClient) GetSchema(connectionName string) (*proto.Schema, error) {
	resp, err := c.Stub.GetSchema(&proto.GetSchemaRequest{Connection: connectionName})
	if err != nil {
		return nil, HandleGrpcError(err, c.Name, "GetSchema")
	}
	return resp.Schema, nil
}

func (c *PluginClient) EstablishMessageStream() (proto.WrapperPlugin_EstablishMessageStreamClient, error) {
	return c.Stub.EstablishMessageStream()
}

// Exited returned whether the underlying client has exited, i.e. the plugin has terminated
func (c *PluginClient) Exited() bool {
	return c.client.Exited()
}

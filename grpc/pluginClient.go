package grpc

import (
	"context"
	"log"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/logging"

	"github.com/hashicorp/go-plugin"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	pluginshared "github.com/turbot/steampipe-plugin-sdk/grpc/shared"
)

// PluginClient is the client object used by clients of the plugin
type PluginClient struct {
	Name   string
	client *plugin.Client
	Stub   pluginshared.WrapperPluginClient
}

func NewPluginClient(reattach *plugin.ReattachConfig, pluginName string, disableLogger bool) (*PluginClient, error) {
	log.Printf("[WARN] NewPluginClient ***************")
	// create the plugin map
	pluginMap := map[string]plugin.Plugin{
		pluginName: &pluginshared.WrapperPlugin{},
	}
	// avoid logging if the plugin is being invoked by refreshConnections
	loggOpts := &hclog.LoggerOptions{Name: "plugin"}
	if disableLogger {
		//loggOpts.Exclude = func(hclog.Level, string, ...interface{}) bool { return true }
	}
	logger := logging.NewLogger(loggOpts)

	// create grpc client
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig:  pluginshared.Handshake,
		Plugins:          pluginMap,
		Reattach:         reattach,
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		Logger:           logger,
	})

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
func (c *PluginClient) SetConnectionConfig(req *proto.SetConnectionConfigRequest) error {
	_, err := c.Stub.SetConnectionConfig(req)
	if err != nil {
		// create a new cleaner error, ignoring Not Implemented errors for backwards compatibility
		return IgnoreNotImplementedError(err, req.ConnectionName, "SetConnectionConfig")
	}
	return nil
}

func (c *PluginClient) GetSupportedOperations() (*proto.GetSupportedOperationsResponse, error) {
	resp, err := c.Stub.GetSupportedOperations(&proto.GetSupportedOperationsRequest{})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *PluginClient) GetSchema() (*proto.Schema, error) {
	resp, err := c.Stub.GetSchema(&proto.GetSchemaRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Schema, nil
}

func (c *PluginClient) Execute(req *proto.ExecuteRequest) (proto.WrapperPlugin_ExecuteClient, context.Context, context.CancelFunc, error) {
	return c.Stub.Execute(req)
}

// Kill kills our underlying GRPC client
func (c *PluginClient) Kill() {
	c.client.Kill()
}

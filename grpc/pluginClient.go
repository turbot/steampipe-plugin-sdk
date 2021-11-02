package grpc

import (
	"context"
	"io/ioutil"
	"log"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	pluginshared "github.com/turbot/steampipe-plugin-sdk/grpc/shared"
	"github.com/turbot/steampipe-plugin-sdk/logging"
)

// PluginClient is the client object used by clients of the plugin
type PluginClient struct {
	Name   string
	client *plugin.Client
	Stub   pluginshared.WrapperPluginClient
	Pid    int
}

func NewPluginClient(reattach *plugin.ReattachConfig, pluginName string) (*PluginClient, error) {
	log.Printf("[TRACE]  NewPluginClient for plugin %s", pluginName)
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
		Pid:    reattach.Pid,
	}
	return res, nil

}
func (c *PluginClient) SetConnectionConfig(req *proto.SetConnectionConfigRequest) error {
	_, err := c.Stub.SetConnectionConfig(req)
	if err != nil {
		// create a new cleaner error, ignoring Not Implemented errors for backwards compatibility
		return HandleGrpcError(err, req.ConnectionName, "SetConnectionConfig")
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

func (c *PluginClient) Execute(req *proto.ExecuteRequest) (str proto.WrapperPlugin_ExecuteClient, ctx context.Context, cancel context.CancelFunc, err error) {
	return c.Stub.Execute(req)
}

// Exited returned whether the underlying client has exited, i.e. th eplugin has terminated
func (c *PluginClient) Exited() bool {
	return c.client.Exited()
}

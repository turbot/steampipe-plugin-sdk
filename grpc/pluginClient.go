package grpc

import (
	"context"

	"github.com/hashicorp/go-plugin"
	pbsdk "github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	pluginshared "github.com/turbot/steampipe-plugin-sdk/grpc/shared"
)

// PluginClient is the client object used by clients of the plugin
type PluginClient struct {
	Name   string
	Path   string
	Client *plugin.Client
	Stub   pluginshared.WrapperPluginClient
}

func (c *PluginClient) SetConnectionConfig(req *pbsdk.SetConnectionConfigRequest) error {
	_, err := c.Stub.SetConnectionConfig(req)
	if err != nil {
		// create a new cleaner error, ignoring Not Implemented errors for backwards compatibility
		return IgnoreNotImplementedError(err, req.ConnectionName, "SetConnectionConfig")
	}
	return nil
}

func (c *PluginClient) GetSchema() (*pbsdk.Schema, error) {
	resp, err := c.Stub.GetSchema(&pbsdk.GetSchemaRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Schema, nil
}

func (c *PluginClient) Execute(req *pbsdk.ExecuteRequest) (pbsdk.WrapperPlugin_ExecuteClient, context.Context, context.CancelFunc, error) {
	return c.Stub.Execute(req)
}

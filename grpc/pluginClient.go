package grpc

import (
	"github.com/hashicorp/go-plugin"
	pluginshared "github.com/turbot/steampipe-plugin-sdk/grpc/shared"
)

// PluginClient:: the client object used by clients of the plugin
type PluginClient struct {
	Name   string
	Path   string
	Client *plugin.Client
	Stub   pluginshared.WrapperPluginClient
}

package plugin

import (
	"context"
	"log"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/logging"
)

// ServeOpts are the configurations to serve a plugin.
type ServeOpts struct {
	PluginName string
	PluginFunc PluginFunc
}

type PluginFunc func(context.Context) *Plugin

func Serve(opts *ServeOpts) {

	ctx := context.WithValue(context.Background(), ContextKeyLogger, logging.NewLogger(&hclog.LoggerOptions{DisableTime: true}))

	// pluginName string, getSchemaFunc GetSchemaFunc, tables map[string]ExecuteFunc
	p := opts.PluginFunc(ctx)

	// NOTE update tables to have a reference to the plugin
	p.claimTables()

	// time will be provided by the plugin logger
	p.Logger = logging.NewLogger(&hclog.LoggerOptions{DisableTime: true})
	log.SetOutput(p.Logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)

	grpc.NewPluginServer(p.Name, p.GetSchema, p.Execute).Serve()
}

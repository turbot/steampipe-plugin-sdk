package plugin

import (
	"context"
	"log"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v3/logging"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/v3/telemetry"
)

// ServeOpts are the configurations to serve a plugin.
type ServeOpts struct {
	PluginName string
	PluginFunc PluginFunc
}

type NewPluginOptions struct {
	ConnectionName   string
	ConnectionConfig string
}
type PluginFunc func(context.Context) *Plugin
type CreatePlugin func(context.Context, string) (*Plugin, error)

func Serve(opts *ServeOpts) {

	ctx := context.WithValue(context.Background(), context_key.Logger, logging.NewLogger(&hclog.LoggerOptions{DisableTime: true}))

	// call plugin function to build a plugin object
	p := opts.PluginFunc(ctx)

	// initialise the plugin - create the connection config map, set plugin pointer on all tables and setup logger
	p.Initialise()

	shutdown, _ := telemetry.Init(p.Name)
	defer func() {
		log.Println("[TRACE] FLUSHING instrumentation")
		//instrument.FlushTraces()
		log.Println("[TRACE] Shutdown instrumentation")
		shutdown()
	}()
	grpc.NewPluginServer(p.Name, p.SetConnectionConfig, p.SetAllConnectionConfigs, p.GetSchema, p.Execute, p.EstablishCacheConnection).Serve()
}

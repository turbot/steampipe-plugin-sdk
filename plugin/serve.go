package plugin

import (
	"context"
	"fmt"
	"log"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/instrument"
	"github.com/turbot/steampipe-plugin-sdk/logging"
	"github.com/turbot/steampipe-plugin-sdk/plugin/context_key"
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

	instrument.InitTracing(fmt.Sprintf("%s-plugin", p.Name), "0.0.0")
	defer func() {
		log.Println("[TRACE] FLUSHING instrumentation")
		instrument.FlushTraces()
		log.Println("[TRACE] Shutdown instrumentation")
		instrument.ShutdownTracing()
	}()

	grpc.NewPluginServer(p.Name, p.SetConnectionConfig, p.GetSchema, p.Execute).Serve()
}

package plugin

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/v4/logging"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/v4/telemetry"
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
	if _, found := os.LookupEnv("STEAMPIPE_PPROF"); found {
		log.Printf("[INFO] PROFILING!!!!")
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	grpc.NewPluginServer(p.Name, p.SetConnectionConfig, p.SetAllConnectionConfigs, p.UpdateConnectionConfigs, p.GetSchema, p.Execute).Serve()
}

package plugin

import (
	"context"
	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
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

/*
	Serve creates and starts the GRPC server which serves the plugin,

passing callback functions to implement each of the plugin interface functions:

  - SetConnectionConfig

  - SetAllConnectionConfigs

  - UpdateConnectionConfigs

  - GetSchema

  - Execute

    It is called from the main function of the plugin.
*/
func Serve(opts *ServeOpts) {
	// create the logger
	logger := setupLogger()

	// add logger into the context for the plugin create func
	ctx := context.WithValue(context.Background(), context_key.Logger, logger)

	// call plugin function to build a plugin object
	p := opts.PluginFunc(ctx)

	// initialise the plugin - create the connection config map, set plugin pointer on all tables
	p.initialise(logger)

	shutdownTelemetry, _ := telemetry.Init(p.Name)
	defer func() {
		log.Println("[TRACE] FLUSHING instrumentation")
		//instrument.FlushTraces()
		log.Println("[TRACE] Shutdown instrumentation")
		shutdownTelemetry()
		p.shutdown()
	}()
	if _, found := os.LookupEnv("STEAMPIPE_PPROF"); found {
		log.Printf("[INFO] PROFILING!!!!")
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}
	// TODO add context into all of these handlers

	grpc.NewPluginServer(p.Name, p.setConnectionConfig, p.setAllConnectionConfigs, p.updateConnectionConfigs, p.getSchema, p.execute, p.establishMessageStream, p.setCacheOptions).Serve()
}

func setupLogger() hclog.Logger {
	// time will be provided by the plugin manager logger
	logger := logging.NewLogger(&hclog.LoggerOptions{DisableTime: true})
	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)
	return logger
}

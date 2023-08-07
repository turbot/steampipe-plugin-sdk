package plugin

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
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
	//
	// go-plugin reads stderr output line-by-line from the plugin instances and sets the level
	// based on the prefix. If there's no level in the prefix, it will set it to a default log level of DEBUG
	// this is a problem for log lines containing "\n", since every line but the first become DEBUG
	// log instead of being part of the actual log line
	//
	// We are using a custom writer here which intercepts the log lines and adds an extra escape to "\n" characters
	//
	// The plugin manager on the other end applies a reverse mapping to get back the original log line
	// https://github.com/turbot/steampipe/blob/742ae17870f7488e1b610bbaf3ddfa852a58bd3e/cmd/plugin_manager.go#L112
	//
	writer := logging.NewLineEscapeWriter(os.Stderr)

	// time will be provided by the plugin manager logger
	logger := logging.NewLogger(&hclog.LoggerOptions{DisableTime: true, Output: writer})
	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)
	return logger
}

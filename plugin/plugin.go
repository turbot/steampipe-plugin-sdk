package plugin

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"syscall"

	"github.com/turbot/steampipe-plugin-sdk/cache"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/go-kit/helpers"
	connection_manager "github.com/turbot/steampipe-plugin-sdk/connection"
	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/logging"
	"github.com/turbot/steampipe-plugin-sdk/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
)

const (
	SchemaModeStatic  = "static"
	SchemaModeDynamic = "dynamic"
)

var validSchemaModes = []string{SchemaModeStatic, SchemaModeDynamic}

// Plugin is an object used to build all necessary data for a given query
type Plugin struct {
	Name     string
	Logger   hclog.Logger
	TableMap map[string]*Table
	// TableMapFunc is a callback function which can be used to populate the table map
	// this con optionally be provided by the plugin, and allows the connection config to be used in the table creation
	// (connection config is not available at plugin creation time)
	TableMapFunc func(ctx context.Context, p *Plugin) (map[string]*Table, error)

	DefaultTransform   *transform.ColumnTransforms
	DefaultGetConfig   *GetConfig
	DefaultConcurrency *DefaultConcurrencyConfig
	DefaultRetryConfig *RetryConfig
	// every table must implement these columns
	RequiredColumns        []*Column
	ConnectionConfigSchema *ConnectionConfigSchema
	// connection this plugin is instantiated for
	Connection *Connection
	// object to handle caching of connection specific data
	ConnectionManager *connection_manager.Manager
	// is this a static or dynamic schema
	SchemaMode string
	Schema     map[string]*proto.TableSchema

	// ConnectionConfigChangedFunc is a callback function executed every time the connection config is updated
	// NOTE: it is NOT executed when it is ste for the first time (???)
	ConnectionConfigChangedFunc func() error
	// query cache
	QueryCache *cache.QueryCache
}

// Initialise initialises the connection config map, set plugin pointer on all tables and setup logger
func (p *Plugin) Initialise() {
	log.Println("[TRACE] Plugin Initialise creating connection manager")
	p.ConnectionManager = connection_manager.NewManager()

	// time will be provided by the plugin logger
	p.Logger = logging.NewLogger(&hclog.LoggerOptions{DisableTime: true})
	log.SetOutput(p.Logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)

	// default the schema mode to static
	if p.SchemaMode == "" {
		p.SchemaMode = SchemaModeStatic
	}

	// set file limit
	p.setuLimit()
}

const uLimitEnvVar = "STEAMPIPE_ULIMIT"
const uLimitDefault = 2560

func (p *Plugin) setuLimit() {
	var ulimit uint64 = uLimitDefault
	if ulimitString, ok := os.LookupEnv(uLimitEnvVar); ok {
		if ulimitEnv, err := strconv.ParseUint(ulimitString, 10, 64); err == nil {
			ulimit = ulimitEnv
		}
	}

	var rLimit syscall.Rlimit
	rLimit.Max = ulimit
	rLimit.Cur = ulimit
	p.Logger.Trace("Setting Ulimit", "ulimit", ulimit)
	err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		p.Logger.Error("Error Setting Ulimit", "error", err)
	}
}

// SetConnectionConfig is always called before any other plugin function
// it parses the connection config string, and populate the connection data for this connection
// it also calls the table creation factory function, if provided by the plugin
func (p *Plugin) SetConnectionConfig(connectionName, connectionConfigString string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("SetConnectionConfig failed: %s", helpers.ToError(r).Error())
		} else {
			p.Logger.Debug("SetConnectionConfig finished")
		}
	}()

	// TODO if the client making this request has an out of date version of the schema, fail
	// if err := p.verifySchemaHash(); err != nil {
	//	return err
	//}
	// ??? NEEDED HERE???

	// create connection object
	p.Connection = &Connection{Name: connectionName}

	// if config was provided, parse it
	if connectionConfigString != "" {
		if p.ConnectionConfigSchema == nil {
			return fmt.Errorf("connection config has been set for connection '%s', but plugin '%s' does not define connection config schema", connectionName, p.Name)
		}
		// ask plugin for a struct to deserialise the config into
		config, err := p.ConnectionConfigSchema.Parse(connectionConfigString)
		if err != nil {
			return err
		}
		p.Connection.Config = config
	}

	// if the plugin defines a CreateTables func, call it now
	ctx := context.WithValue(context.Background(), context_key.Logger, p.Logger)
	if err := p.initialiseTables(ctx); err != nil {
		return err
	}

	// populate the plugin schema
	p.schema, err = p.populateSchema()
	if err != nil {
		return err
	}

	// if we have not created the query cache yet, create now
	if p.QueryCache == nil {
		queryCache, err := cache.NewQueryCache(p.Connection.Name, p.schema)
		if err != nil {
			return err
		}

		p.QueryCache = queryCache
	}

	// if plugin provided a connection config callback function, call it
	err = p.onConnectionConfigChange()
	return err
}

// initialiseTables does 2 things:
// 1) if a TableMapFunc factory function was provided by the plugin, call it
// 2) update tables to have a reference to the plugin
func (p *Plugin) initialiseTables(ctx context.Context) (err error) {
	if p.TableMapFunc != nil {
		// handle panic in factory function
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("failed to plugin initialise plugin '%s': TableMapFunc '%s' had unhandled error: %v", p.Name, helpers.GetFunctionName(p.TableMapFunc), helpers.ToError(r))
			}
		}()

		if tableMap, err := p.TableMapFunc(ctx, p); err != nil {
			return err
		} else {
			p.TableMap = tableMap
		}
	}

	// update tables to have a reference to the plugin
	for _, table := range p.TableMap {
		table.Plugin = p
	}

	// now validate the plugin
	// NOTE: must do this after calling TableMapFunc
	if validationErrors := p.Validate(); validationErrors != "" {
		return fmt.Errorf("plugin %s validation failed: \n%s", p.Name, validationErrors)
	}
	return nil
}

func (p *Plugin) GetSchema() (*grpc.PluginSchema, error) {
	// the connection property must be set already
	if p.Connection == nil {
		return nil, fmt.Errorf("plugin.GetSchema called before setting connection config")
	}

	schema := &grpc.PluginSchema{Schema: p.schema, Mode: p.SchemaMode}
	return schema, nil
}

// Execute executes a query and stream the results
func (p *Plugin) Execute(req *proto.ExecuteRequest, stream proto.WrapperPlugin_ExecuteServer) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()

	// TODO if the client making this request has an out of date version of the schema, fail
	// if err := p.verifySchemaHash(); err != nil {
	//	return err
	//}

	// the connection property must be set already
	if p.Connection == nil {
		return fmt.Errorf("plugin.Execute called before setting connection config")
	}

	logging.LogTime("Start execute")
	p.Logger.Trace("Execute ", "connection", req.Connection, "connection config", p.Connection, "table", req.Table)

	queryContext := NewQueryContext(req.QueryContext)
	table, ok := p.TableMap[req.Table]
	if !ok {
		return fmt.Errorf("plugin %s does not provide table %s", p.Name, req.Table)
	}

	p.Logger.Trace("Got query context",
		"table", req.Table,
		"cols", queryContext.Columns)

	// async approach
	// 1) call list() in a goroutine. This writes pages of items to the rowDataChan. When complete it closes the channel
	// 2) range over rowDataChan - for each item spawn a goroutine to build a row
	// 3) Build row spawns goroutines for any required hydrate functions.
	// 4) When hydrate functions are complete, apply transforms to generate column values. When row is ready, send on rowChan
	// 5) Range over rowChan - for each row, send on results stream
	ctx := context.WithValue(stream.Context(), context_key.Logger, p.Logger)

	var matrixItem []map[string]interface{}

	// get the matrix item
	if table.GetMatrixItem != nil {
		matrixItem = table.GetMatrixItem(ctx, p.Connection)
	}

	queryData := newQueryData(queryContext, table, stream, p.Connection, matrixItem, p.ConnectionManager)
	p.Logger.Trace("calling fetchItems", "table", table.Name, "matrixItem", queryData.Matrix, "limit", queryContext.Limit)

	// asyncronously fetch items
	if err := table.fetchItems(ctx, queryData); err != nil {
		p.Logger.Warn("fetchItems returned an error", "table", table.Name, "error", err)
		return err
	}
	logging.LogTime("Calling build Rows")

	// asyncronously build rows
	rowChan := queryData.buildRows(ctx)
	logging.LogTime("Calling build Stream")
	// asyncronously stream rows
	return queryData.streamRows(ctx, rowChan)
}

// if query cache does not exist, create
// if the query cache exists, update the schema
func (p *Plugin) onConnectionConfigChange() error {
	if p.QueryCache != nil {
		// so there is already a cache - that means the config has been updated, not set for the first time

		// update the schema on the query cache
		p.QueryCache.PluginSchema = p.schema

		// if the plugin defines a connection config changed callback cal it
		if p.ConnectionConfigChangedFunc != nil {
			return p.ConnectionConfigChangedFunc()
		}
		return nil
	}

	queryCache, err := cache.NewQueryCache(p.Connection.Name, p.schema)
	if err != nil {
		return err
	}

	p.QueryCache = queryCache
	return nil
}

func (p *Plugin) populateSchema() (map[string]*proto.TableSchema, error) {
	// the connection property must be set already
	if p.Connection == nil {
		return nil, fmt.Errorf("plugin.GetSchema called before setting connection config")
	}
	schema := map[string]*proto.TableSchema{}

	var tables []string
	for tableName, table := range p.TableMap {
		schema[tableName] = table.GetSchema()
		tables = append(tables, tableName)
	}
	// TODO set schema hash

	return schema, nil
}

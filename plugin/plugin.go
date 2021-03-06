package plugin

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"syscall"

	connection_manager "github.com/turbot/steampipe-plugin-sdk/connection"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/logging"
	"github.com/turbot/steampipe-plugin-sdk/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
)

// Plugin is an object used to build all necessary data for a given query
type Plugin struct {
	Name               string
	Logger             hclog.Logger
	TableMap           map[string]*Table
	DefaultTransform   *transform.ColumnTransforms
	DefaultGetConfig   *GetConfig
	DefaultConcurrency *DefaultConcurrencyConfig
	DefaultRetryConfig *RetryConfig
	// every table must implement these columns
	RequiredColumns        []*Column
	ConnectionConfigSchema *ConnectionConfigSchema
	// a map of connection name to connection structs
	Connections map[string]*Connection
	// object to handle caching of connection specific data
	ConnectionManager *connection_manager.Manager
}

// Initialise initialises the connection config map, set plugin pointer on all tables and setup logger
func (p *Plugin) Initialise() {
	//  initialise the connection map
	p.Connections = make(map[string]*Connection)
	log.Println("[TRACE] Plugin Initialise creating connection manager")
	p.ConnectionManager = connection_manager.NewManager()

	// NOTE update tables to have a reference to the plugin
	p.claimTables()

	// time will be provided by the plugin logger
	p.Logger = logging.NewLogger(&hclog.LoggerOptions{DisableTime: true})
	log.SetOutput(p.Logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)

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

func (p *Plugin) GetSchema() (map[string]*proto.TableSchema, error) {
	// first validate the plugin
	if validationErrors := p.Validate(); validationErrors != "" {
		return nil, fmt.Errorf("plugin %s validation failed: \n%s", p.Name, validationErrors)
	}

	schema := map[string]*proto.TableSchema{}

	for tableName, table := range p.TableMap {
		schema[tableName] = table.GetSchema()
	}
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

	logging.LogTime("Start execute")
	p.Logger.Debug("Execute ", "connection", req.Connection, "connection config", p.Connections, "table", req.Table)

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
	var connection *Connection

	// NOTE: req.Connection parameter is only populated in version 0.2.0 of Steampipe - check whether it exists
	if req.Connection != "" {
		connection = p.Connections[req.Connection]
	}

	// get the matrix item
	if table.GetMatrixItem != nil {
		matrixItem = table.GetMatrixItem(ctx, connection)
	}

	queryData := newQueryData(queryContext, table, stream, connection, matrixItem, p.ConnectionManager)
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

// SetConnectionConfig parses the connection config string, and populate the connection data for this connection
// NOTE: we always pass and store connection config BY VALUE
func (p *Plugin) SetConnectionConfig(connectionName, connectionConfigString string) (err error) {

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("SetConnectionConfig failed: %s", ToError(r).Error())
		} else {
			p.Logger.Debug("SetConnectionConfig finished")
		}
	}()

	// first validate the plugin
	if validationErrors := p.Validate(); validationErrors != "" {
		return fmt.Errorf("plugin %s validation failed: \n%s", p.Name, validationErrors)
	}
	if connectionConfigString == "" {
		return nil
	}
	if p.ConnectionConfigSchema == nil {
		return fmt.Errorf("connection config has been set for connection '%s', but plugin '%s' does not define connection config schema", connectionName, p.Name)
	}

	// ask plugin for a struct to deserialise the config into
	config, err := p.ConnectionConfigSchema.Parse(connectionConfigString)
	if err != nil {
		return err
	}
	p.Connections[connectionName] = &Connection{connectionName, config}
	return nil
}

// slightly hacky - called on startup to set a plugin pointer in each table
func (p *Plugin) claimTables() {
	for _, table := range p.TableMap {
		table.Plugin = p
	}
}

package plugin

import (
	"context"
	"fmt"
	"log"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/logging"
	"github.com/turbot/steampipe-plugin-sdk/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
)

// Plugin :: an object used to build all necessary data for a given query
type Plugin struct {
	Name               string
	Logger             hclog.Logger
	TableMap           map[string]*Table
	DefaultTransform   *transform.ColumnTransforms
	DefaultGetConfig   *GetConfig
	DefaultConcurrency *DefaultConcurrencyConfig
	// every table must implement these columns
	RequiredColumns  []*Column
	ConnectionConfig *ConnectionConfig
}

// Initialise :: initialise the connection config map, set plugin pointer on all tables and setup logger
func (p *Plugin) Initialise() {
	// if no connection config is defined, create an empty one
	if p.ConnectionConfig == nil {
		p.ConnectionConfig = NewConnectionConfig()
	} else {
		// otherwise just initialise the connection config map
		p.ConnectionConfig.ConfigMap = make(map[string]*Connection)
	}

	// NOTE update tables to have a reference to the plugin
	p.claimTables()

	// time will be provided by the plugin logger
	p.Logger = logging.NewLogger(&hclog.LoggerOptions{DisableTime: true})
	log.SetOutput(p.Logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)
}

func (p *Plugin) GetSchema() (map[string]*proto.TableSchema, error) {
	// first validate the plugin - this will panic with a validation error
	if validationErrors := p.Validate(); validationErrors != "" {
		return nil, fmt.Errorf("plugin %s validation failed: \n%s", p.Name, validationErrors)
	}

	schema := map[string]*proto.TableSchema{}

	for tableName, table := range p.TableMap {
		schema[tableName] = table.GetSchema()
	}
	return schema, nil
}

// Execute :: execute a query and stream the results
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
	p.Logger.Debug("Execute ", "connection", req.Connection, "connection config", p.ConnectionConfig.ConfigMap)

	queryContext := req.QueryContext
	table, ok := p.TableMap[req.Table]
	if !ok {
		return fmt.Errorf("plugin %s does not provide table %s", p.Name, req.Table)
	}

	p.Logger.Debug("Got query context",
		"cols", queryContext.Columns,
		"quals", grpc.QualMapToString(queryContext.Quals))

	// async approach
	// 1) call list() in a goroutine. This writes pages of items to the rowDataChan. When complete it closes the channel
	// 2) range over rowDataChan - for each item spawn a goroutine to build a row
	// 3) Build row spawns goroutines for any required hydrate functions.
	// 4) When hydrate functions are complete, apply transforms to generate column values. When row is ready, send on rowChan
	// 5) Range over rowChan - for each row, send on results stream
	ctx := context.WithValue(context.Background(), context_key.Logger, p.Logger)

	var fetchMetadata = make([]map[string]interface{}, 0)
	var connection *Connection

	// NOTE: req.Connection parameter is only populated in version 0.2.0 of Steampipe - check whether it exists
	if req.Connection != "" {
		connectionConfig := p.ConnectionConfig.ConfigMap[req.Connection]
		connection = &Connection{req.Connection, connectionConfig}
	}
	queryData := newQueryData(queryContext, table, stream, connection, fetchMetadata)

	// get the fetch metadata
	if table.GetFetchMetadata != nil {
		fetchMetadata = table.GetFetchMetadata(ctx, queryData)
	}
	p.Logger.Debug("calling fetchItems", "table", table.Name)

	// asyncronously fetch items
	if err := table.fetchItems(ctx, queryData); err != nil {
		return err
	}
	log.Println("[TRACE] after fetchItems")
	logging.LogTime("Calling stream")

	// asyncronously build rows
	rowChan := queryData.buildRows(ctx)
	// asyncronously stream rows
	return queryData.streamRows(ctx, rowChan)
}

func (p *Plugin) SetConnectionConfig(connectionName, connectionConfigString string) error {
	return p.ConnectionConfig.SetConnectionConfig(connectionName, connectionConfigString)
}

// slightly hacky - called on startup to set a plugin pointer in each table
func (p *Plugin) claimTables() {
	for _, table := range p.TableMap {
		table.Plugin = p
	}
}

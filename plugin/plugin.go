package plugin

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v3/cache"
	connection_manager "github.com/turbot/steampipe-plugin-sdk/v3/connection"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v3/logging"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin/os_specific"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin/transform"
	"github.com/turbot/steampipe-plugin-sdk/v3/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	SchemaModeStatic  = "static"
	SchemaModeDynamic = "dynamic"
	uLimitEnvVar      = "STEAMPIPE_ULIMIT"
	uLimitDefault     = 2560
)

var validSchemaModes = []string{SchemaModeStatic, SchemaModeDynamic}

// Plugin is a struct used define the GRPC plugin.
//
// This includes the plugin schema (i.e. the tables provided by the plugin),
// as well as config for the default error handling and concurrency behaviour.
//
// By convention, the package name for your plugin should be the same name as your plugin,
// and go files for your plugin (except main.go) should reside in a folder with the same name.
type Plugin struct {
	Name   string
	Logger hclog.Logger
	// TableMap is a map of all the tables in the plugin, keyed by the table name
	TableMap map[string]*Table
	// TableMapFunc is a callback function which can be used to populate the table map
	// this con optionally be provided by the plugin, and allows the connection config to be used in the table creation
	// (connection config is not available at plugin creation time)
	TableMapFunc        func(ctx context.Context, p *Plugin) (map[string]*Table, error)
	DefaultTransform    *transform.ColumnTransforms
	DefaultConcurrency  *DefaultConcurrencyConfig
	DefaultRetryConfig  *RetryConfig
	DefaultIgnoreConfig *IgnoreConfig

	// deprecated - use DefaultRetryConfig and DefaultIgnoreConfig
	DefaultGetConfig *GetConfig
	// deprecated - use DefaultIgnoreConfig
	DefaultShouldIgnoreError ErrorPredicate
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

	queryCache      *cache.QueryCache
	concurrencyLock sync.Mutex
	// the cache stream - this is set in EstablishCacheConnection and used  when creating thew query cache in ensureCache
	cacheStream proto.WrapperPlugin_EstablishCacheConnectionServer
}

// Initialise creates the 'connection manager' (which provides caching), sets up the logger
// and sets the file limit.
func (p *Plugin) Initialise() {
	log.Println("[TRACE] Plugin Initialise creating connection manager")
	p.ConnectionManager = connection_manager.NewManager()

	p.Logger = p.setupLogger()
	// default the schema mode to static
	if p.SchemaMode == "" {
		log.Println("[TRACE] defaulting SchemaMode to SchemaModeStatic")
		p.SchemaMode = SchemaModeStatic
	}

	// create DefaultRetryConfig if needed
	if p.DefaultRetryConfig == nil {
		log.Printf("[TRACE] no DefaultRetryConfig defined - creating empty")
		p.DefaultRetryConfig = &RetryConfig{}
	}

	// create DefaultIgnoreConfig if needed
	if p.DefaultIgnoreConfig == nil {
		log.Printf("[TRACE] no DefaultIgnoreConfig defined - creating empty")
		p.DefaultIgnoreConfig = &IgnoreConfig{}
	}
	// copy the (deprecated) top level ShouldIgnoreError property into the ignore config
	if p.DefaultShouldIgnoreError != nil && p.DefaultIgnoreConfig.ShouldIgnoreError == nil {
		p.DefaultIgnoreConfig.ShouldIgnoreError = p.DefaultShouldIgnoreError
	}

	// if there is a default get config, initialise it
	// (this ensures we handle the deprecated ShouldIgnoreError property)
	if p.DefaultGetConfig != nil {
		log.Printf("[TRACE] intialising DefaultGetConfig")
		// pass nil as the table to indicate this is the plugin default
		p.DefaultGetConfig.initialise(nil)
	}

	// set file limit
	p.setuLimit()
}

// SetConnectionConfig is the handler function for the GetSchema grpc function
// parse the connection config string, and populate the connection data for this connection.
// It also calls the table creation factory function, if provided by the plugin.
// Note: SetConnectionConfig is always called before any other plugin function except EstablishCacheConnection.
func (p *Plugin) SetConnectionConfig(connectionName, connectionConfigString string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("SetConnectionConfig failed: %s", helpers.ToError(r).Error())
		} else {
			p.Logger.Debug("SetConnectionConfig finished")
		}
	}()

	log.Printf("[TRACE] SetConnectionConfig connection '%s'", connectionName)

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
	p.Schema, err = p.buildSchema()
	if err != nil {
		return err
	}

	// create the cache or update the schema if it already exists
	return p.ensureCache()
}

// GetSchema is the handler function for the GetSchema grpc function
// return the plugin schema.
// Note: the connection config must be set before calling this function.
func (p *Plugin) GetSchema() (*grpc.PluginSchema, error) {
	// the connection property must be set already
	if p.Connection == nil {
		return nil, fmt.Errorf("plugin.GetSchema called before setting connection config")
	}

	schema := &grpc.PluginSchema{Schema: p.Schema, Mode: p.SchemaMode}
	return schema, nil
}

// Execute is the handler function for the Execute grpc function
// execute a query and streams the results using the given GRPC stream.
func (p *Plugin) Execute(req *proto.ExecuteRequest, stream proto.WrapperPlugin_ExecuteServer) (err error) {
	// add CallId to logs for the execute call
	logger := p.Logger.Named(req.CallId)

	log.Printf("[TRACE] EXECUTE callId: %s table: %s cols: %s", req.CallId, req.Table, strings.Join(req.QueryContext.Columns, ","))

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] Execute recover from panic: callId: %s table: %s error: %v", req.CallId, req.Table, r)
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("%v", r)
			}
			return
		}

		log.Printf("[TRACE] Execute complete callId: %s table: %s ", req.CallId, req.Table)
	}()

	// the connection property must be set already
	if p.Connection == nil {
		return fmt.Errorf("plugin.Execute called before setting connection config")
	}

	logging.LogTime("Start execute")
	logger.Trace("Execute ", "connection", req.Connection, "table", req.Table)

	queryContext := NewQueryContext(req.QueryContext)
	table, ok := p.TableMap[req.Table]
	if !ok {
		return fmt.Errorf("plugin %s does not provide table %s", p.Name, req.Table)
	}

	logger.Trace("Got query context",
		"table", req.Table,
		"cols", queryContext.Columns)

	// async approach
	// 1) call list() in a goroutine. This writes pages of items to the rowDataChan. When complete it closes the channel
	// 2) range over rowDataChan - for each item spawn a goroutine to build a row
	// 3) Build row spawns goroutines for any required hydrate functions.
	// 4) When hydrate functions are complete, apply transforms to generate column values. When row is ready, send on rowChan
	// 5) Range over rowChan - for each row, send on results stream
	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)

	// get a context which includes telemetry data and logger
	ctx := p.buildExecuteContext(stream.Context(), req, logger)

	log.Printf("[TRACE] Start execute span")
	ctx, executeSpan := p.startExecuteSpan(ctx, req)
	defer func() {
		log.Printf("[TRACE] End execute span")
		executeSpan.End()
	}()

	// get the matrix item
	var matrixItem []map[string]interface{}
	if table.GetMatrixItem != nil {
		matrixItem = table.GetMatrixItem(ctx, p.Connection)
	}

	queryData := p.newQueryData(req, stream, queryContext, table, matrixItem)

	logger.Trace("calling fetchItems", "table", table.Name, "matrixItem", queryData.Matrix, "limit", queryContext.Limit)

	// convert limit from *int64 to an int64 (where -1 means no limit)
	var limit int64 = -1
	if queryContext.Limit != nil {
		limit = *queryContext.Limit
	}
	// can we satisfy this request from the cache?
	if req.CacheEnabled && p.queryCache == nil {
		return fmt.Errorf("no cache connection has been established with the plugin manager")
	}

	if req.CacheEnabled && p.queryCache != nil {
		log.Printf("[TRACE] Cache ENABLED callId: %s %p", req.CallId, p.queryCache)
		// try to fetch this data from the query cache
		// if it is a cache hit, the data will be streamed  by cacheGet
		cacheHit := p.cacheGet(req, ctx, table, queryContext, limit, executeSpan, queryData)
		if cacheHit {
			// nothing more to do
			return nil
		}

		// so cache is enabled but the data is not in the cache
		cacheSetCallId, err := p.queryCache.StartSet(table.Name, queryContext.UnsafeQuals, queryContext.Columns, limit, req.CacheTtl)
		// if StartSet failed, just log the error but continue
		if err != nil {
			log.Printf("[WARN] queryCache.StartSet failed - this query will not be cached: %s", err.Error())
		} else {
			// save callId to querydata
			queryData.cacheSetCallId = cacheSetCallId
		}

		// the cache will have added a pending item for this transfer
		// and it is our responsibility to either call 'set' or 'cancel' for this pending item
		defer func() {
			if err != nil || ctx.Err() != nil {
				log.Printf("[WARN] Execute call failed err: %v cancelled: %v - cancelling pending item in cache", err, ctx.Err())
				p.queryCache.CancelPendingItem(table.Name, queryContext.UnsafeQuals, queryContext.Columns, limit)
			}
		}()
	} else {
		log.Printf("[TRACE] Cache DISABLED callId: %s", req.CallId)
	}

	log.Printf("[TRACE] fetch items callId: %s", req.CallId)
	// asyncronously fetch items
	if err := table.fetchItems(ctx, queryData); err != nil {
		logger.Warn("fetchItems returned an error", "table", table.Name, "error", err)
		return err
	}
	logging.LogTime("Calling build Rows")

	log.Printf("[TRACE] buildRows callId: %s", req.CallId)

	// asyncronously build rows
	rowChan := queryData.buildRows(ctx)

	log.Printf("[TRACE] streamRows callId: %s", req.CallId)

	logging.LogTime("Calling streamRows")

	// asyncronously stream rows across GRPC
	err = queryData.streamRows(ctx, rowChan)
	if err != nil {
		return err
	}

	//if req.CacheEnabled {
	//	log.Printf("[TRACE] queryCache.Set callId: %s", req.CallId)
	//
	//	//cacheResult := &proto.QueryResult{Rows: rows}
	//	p.queryCache.Set(table.Name, queryContext.UnsafeQuals, queryContext.Columns, limit, cacheResult)
	//}

	return nil
}

func (p *Plugin) cacheGet(req *proto.ExecuteRequest, ctx context.Context, table *Table, queryContext *QueryContext, limit int64, executeSpan trace.Span, queryData *QueryData) bool {

	var doneChan = make(chan (bool))
	callback := func(resp proto.CacheResponse) {
		if !resp.Success {
			doneChan <- false
		}
		if len(resp.QueryResult.Rows) == 0 {
			executeSpan.SetAttributes(
				attribute.Bool("cache-hit", true),
			)

			queryData.QueryStatus.cacheHit = true
			doneChan <- true
		}
		for _, r := range resp.QueryResult.Rows {
			queryData.streamRow(r)
			queryData.QueryStatus.cachedRowsFetched++
		}
	}

	cacheHit, err := p.queryCache.Get(ctx, table.Name, queryContext.UnsafeQuals, queryContext.Columns, limit, req.CacheTtl, callback)
	if err != nil {
		log.Printf("[WARN] cache Get returned an error: %v", err)
		return false
	}

}

// EstablishCacheConnection is the handler function for the EstablishCacheConnection grpc function
// it will be called _before_ SetConnectionConfig, which creates the local QueryCache
// store the stream, then keep it open, i.e. do not return from this function
// Note: SetConnectionConfig is always called immediately after instantiation
func (p *Plugin) EstablishCacheConnection(stream proto.WrapperPlugin_EstablishCacheConnectionServer) error {
	log.Printf("[WARN] EstablishCacheConnection!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	p.cacheStream = stream
	if p.queryCache != nil {
		// TODO do we need to clear the map of callbacks - or invoke all their error functions
		// - as the cache may have lost connection
		// maybe we are reestablishing connection
		log.Printf("[WARN] QUERY CACHE ALREADY CREATED")
		p.queryCache.SetCacheStream(stream)
	}
	// hold stream open
	for {
	}
	return nil
}

func (p *Plugin) buildExecuteContext(ctx context.Context, req *proto.ExecuteRequest, logger hclog.Logger) context.Context {
	// create a traceable context from the stream context
	log.Printf("[TRACE] calling ExtractContextFromCarrier")
	ctx = grpc.ExtractContextFromCarrier(ctx, req.TraceContext)
	// add logger to context
	return context.WithValue(ctx, context_key.Logger, logger)
}

func (p *Plugin) startExecuteSpan(ctx context.Context, req *proto.ExecuteRequest) (context.Context, trace.Span) {
	ctx, span := telemetry.StartSpan(ctx, p.Name, "Plugin.Execute (%s)", req.Table)

	span.SetAttributes(
		attribute.Bool("cache-enabled", req.CacheEnabled),
		attribute.Int64("cache-ttl", req.CacheTtl),
		attribute.String("connection", req.Connection),
		attribute.String("call-id", req.CallId),
		attribute.String("table", req.Table),
		attribute.StringSlice("columns", req.QueryContext.Columns),
		attribute.String("quals", grpc.QualMapToString(req.QueryContext.Quals, false)),
	)
	if req.QueryContext.Limit != nil {
		span.SetAttributes(attribute.Int64("limit", req.QueryContext.Limit.Value))
	}
	return ctx, span
}

func (p *Plugin) newQueryData(req *proto.ExecuteRequest, stream proto.WrapperPlugin_ExecuteServer, queryContext *QueryContext, table *Table, matrixItem []map[string]interface{}) *QueryData {
	// lock access to the newQueryData - otherwise plugin crashes were observed
	p.concurrencyLock.Lock()
	defer p.concurrencyLock.Unlock()

	return newQueryData(queryContext, table, stream, p.Connection, matrixItem, p.ConnectionManager, req.CallId)
}

// initialiseTables does 2 things:
// 1) if a TableMapFunc factory function was provided by the plugin, call it
// 2) call initialise on the table, plassing the plugin pointer which the table stores
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
		table.initialise(p)
	}

	// now validate the plugin
	// NOTE: must do this after calling TableMapFunc
	if validationErrors := p.Validate(); validationErrors != "" {
		return fmt.Errorf("plugin %s validation failed: \n%s", p.Name, validationErrors)
	}
	return nil
}

func (p *Plugin) setupLogger() hclog.Logger {
	// time will be provided by the plugin manager logger
	logger := logging.NewLogger(&hclog.LoggerOptions{DisableTime: true})
	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)
	return logger
}

func (p *Plugin) setuLimit() {
	var ulimit uint64 = uLimitDefault
	if ulimitString, ok := os.LookupEnv(uLimitEnvVar); ok {
		if ulimitEnv, err := strconv.ParseUint(ulimitString, 10, 64); err == nil {
			ulimit = ulimitEnv
		}
	}
	err := os_specific.SetRlimit(ulimit, p.Logger)
	if err != nil {
		p.Logger.Error("Error Setting Ulimit", "error", err)
	}
}

// if query cache does not exist, create
// if the query cache exists, update the schema
func (p *Plugin) ensureCache() error {
	if p.queryCache == nil {
		queryCache, err := cache.NewQueryCache(p.Name, p.Connection.Name, p.Schema, p.cacheStream)
		if err != nil {
			return err
		}
		p.queryCache = queryCachecache jit
	} else {
		// so there is already a cache - that means the config has been updated, not set for the first time

		// update the schema on the query cache
		p.queryCache.PluginSchema = p.Schema
	}

	return nil
}

func (p *Plugin) buildSchema() (map[string]*proto.TableSchema, error) {
	// the connection property must be set already
	if p.Connection == nil {
		return nil, fmt.Errorf("plugin.GetSchema called before setting connection config")
	}
	schema := map[string]*proto.TableSchema{}

	var tables []string
	for tableName, table := range p.TableMap {
		tableSchema, err := table.GetSchema()
		if err != nil {
			return nil, err
		}
		schema[tableName] = tableSchema
		tables = append(tables, tableName)
	}

	return schema, nil
}

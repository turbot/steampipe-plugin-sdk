package plugin

import (
	"context"
	"fmt"
	"github.com/dgraph-io/ristretto"
	"github.com/eko/gocache/v3/cache"
	"github.com/eko/gocache/v3/store"
	"github.com/hashicorp/go-hclog"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v3/logging"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin/os_specific"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin/transform"
	"github.com/turbot/steampipe-plugin-sdk/v3/query_cache"
	"github.com/turbot/steampipe-plugin-sdk/v3/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"log"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
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
	// NOTE: it must be NULL for plugins with dynamic schema
	TableMap map[string]*Table
	// TableMapFunc is a callback function which can be used to populate the table map
	// this con optionally be provided by the plugin, and allows the connection config to be used in the table creation
	// (connection config is not available at plugin creation time)
	TableMapFunc        func(ctx context.Context, p *Plugin, connection *Connection) (map[string]*Table, error)
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

	// map of connection data (schema, config, connection cache)
	// keyed by connection name
	ConnectionMap map[string]*ConnectionData
	// is this a static or dynamic schema
	SchemaMode string

	queryCache *query_cache.QueryCache
	// shared connection cache - this is the underlying cache used for all queryData ConnectionCache
	connectionCacheStore *cache.Cache[any]

	concurrencyLock sync.Mutex
	// the cache stream - this is set in EstablishCacheConnection and used  when creating thew query cache in ensureCache
	//cacheStream proto.WrapperPlugin_EstablishCacheConnectionServer
}

// Initialise creates the 'connection manager' (which provides caching), sets up the logger
// and sets the file limit.
func (p *Plugin) Initialise() {
	log.Println("[TRACE] Plugin Initialise creating connection manager")
	p.ConnectionMap = make(map[string]*ConnectionData)

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

	if err := p.createConnectionCacheStore(); err != nil {
		panic(fmt.Sprintf("failed to create connection cache: %s", err.Error()))
	}

	p.ensureCache()
}

func (p *Plugin) createConnectionCacheStore() error {
	ristrettoCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1000,
		MaxCost:     100000,
		BufferItems: 64,
	})
	if err != nil {
		return err
	}
	ristrettoStore := store.NewRistretto(ristrettoCache)
	p.connectionCacheStore = cache.New[any](ristrettoStore)
	return nil
}

func (p *Plugin) SetConnectionConfig(connectionName, connectionConfigString string) (err error) {
	return p.SetAllConnectionConfigs([]*proto.ConnectionConfig{
		{
			Connection: connectionName,
			Config:     connectionConfigString,
		},
	})
}

func (p *Plugin) SetAllConnectionConfigs(configs []*proto.ConnectionConfig) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("SetAllConnectionConfigs failed: %s", helpers.ToError(r).Error())
		} else {
			p.Logger.Debug("SetAllConnectionConfigs finished")
		}
	}()

	log.Printf("[WARN] SetAllConnectionConfigs %d configs", len(configs))

	// if this plugin does not have dynamic config, we can share table map and schema
	var exemplarSchema map[string]*proto.TableSchema
	var exemplarTableMap map[string]*Table

	for _, config := range configs {
		connectionName := config.Connection

		connectionConfigString := config.Config
		if connectionName == "" {
			log.Printf("[WARN] SetAllConnectionConfigs failed - ConnectionConfig contained empty connection name")
			return fmt.Errorf("SetAllConnectionConfigs failed - ConnectionConfig contained empty connection name")
		}

		// create connection object
		c := &Connection{Name: connectionName}

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
			c.Config = config
		}

		schema := exemplarSchema
		tableMap := exemplarTableMap
		var err error

		if tableMap == nil {
			log.Printf("[WARN] connection %s build schema and table map", connectionName)
			// if the plugin defines a CreateTables func, call it now
			ctx := context.WithValue(context.Background(), context_key.Logger, p.Logger)
			tableMap, err = p.initialiseTables(ctx, c)
			if err != nil {
				return err
			}

			// populate the plugin schema
			schema, err = p.buildSchema(tableMap)
			if err != nil {
				return err
			}

			if p.SchemaMode == SchemaModeStatic {
				exemplarSchema = schema
				exemplarTableMap = tableMap
			}
		}

		// add to connection map
		p.ConnectionMap[connectionName] = &ConnectionData{
			TableMap:   tableMap,
			Connection: c,
			Schema:     schema,
		}
	}
	return nil
}

// GetSchema is the handler function for the GetSchema grpc function
// return the plugin schema.
// Note: the connection config must be set before calling this function.
func (p *Plugin) GetSchema(connectionName string) (*grpc.PluginSchema, error) {
	var connectionData *ConnectionData
	if connectionName == "" {
		// TACTICAL
		// previous steampipe versions do not pass a connection name
		// and instantiate a plugin per connection,
		// is we have more than one connection, this is an error
		if len(p.ConnectionMap) > 1 {
			return nil, fmt.Errorf("Plugin.GetSchema failed - no connection name passed and multiple connections loaded")
		}
		// get first (and only) connection data
		for _, connectionData = range p.ConnectionMap {
		}
	} else {
		var ok bool
		connectionData, ok = p.ConnectionMap[connectionName]
		if !ok {
			return nil, fmt.Errorf("Plugin.GetSchema failed - no connection data loaded for connection '%s'", connectionName)
		}
	}
	schema := &grpc.PluginSchema{Schema: connectionData.Schema, Mode: p.SchemaMode}
	return schema, nil
}

// Execute is the handler function for the Execute grpc function
// execute a query and streams the results using the given GRPC stream.
func (p *Plugin) Execute(req *proto.ExecuteRequest, stream proto.WrapperPlugin_ExecuteServer) (err error) {
	defer log.Printf("[WARN] EXECUTE DONE************************")

	outputChan := make(chan *proto.ExecuteResponse, len(req.ExecuteConnectionData))
	errorChan := make(chan error, len(req.ExecuteConnectionData))
	doneChan := make(chan bool)
	var outputWg sync.WaitGroup

	// add CallId to logs for the execute call
	logger := p.Logger.Named(req.CallId)
	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)

	// get a context which includes telemetry data and logger
	ctx := p.buildExecuteContext(stream.Context(), req, logger)

	for connectionName, executeData := range req.ExecuteConnectionData {
		outputWg.Add(1)
		go func(c string) {
			defer outputWg.Done()
			if err := p.executeForConnection(ctx, req, c, executeData, outputChan); err != nil {
				log.Printf("[WARN] executeForConnection %s returned error %s", c, err.Error())
				errorChan <- err
			}
			log.Printf("[WARN] executeForConnection %s returned", c)
		}(connectionName)
	}

	var errors []error

	go func() {
		outputWg.Wait()
		close(doneChan)
	}()

	complete := false
	for !complete {
		select {
		case row := <-outputChan:
			// nil row means that connection is done streaming
			if row == nil {
				// continue looping
				break
			}

			if err := stream.Send(row); err != nil {
				// cancel any ongoing operations
				// abort any ongoing cache set operation in the cache server
				//d.abortCacheSet()
				log.Printf("[ERROR] Execute - streamRow returned an error %s\n", err)
				log.Printf("[WARNING] WAITING FOR DONE")

				// HACKY MCHACK
				for !complete {
					select {
					case <-errorChan:
						log.Printf("[WARN] error chan select")
					case <-doneChan:
						log.Printf("[WARN] done chan select")
						complete = true
					}
				}
				errors = append(errors, err)
				break
			}
		case err := <-errorChan:
			log.Printf("[WARN] error channel received %s", err.Error())
			errors = append(errors, err)
		case <-doneChan:
			log.Printf("[WARN] done, closing channels")
			complete = true
		}
	}

	close(outputChan)
	close(errorChan)

	return helpers.CombineErrors(errors...)
}

func (p *Plugin) executeForConnection(ctx context.Context, req *proto.ExecuteRequest, connectionName string, executeData *proto.ExecuteConnectionData, outputChan chan *proto.ExecuteResponse) (err error) {
	const rowBufferSize = 10
	var rowChan = make(chan *proto.Row, rowBufferSize)
	// get limit and cache vars
	limitParam := executeData.Limit
	cacheTTL := executeData.CacheTtl
	cacheEnabled := executeData.CacheEnabled

	log.Printf("[TRACE] EXECUTE callId: %s connection: %s table: %s cols: %s", req.CallId, connectionName, req.Table, strings.Join(req.QueryContext.Columns, ","))

	defer func() {
		log.Printf("[TRACE] executeForConnection RETURNING %s ***************", connectionName)
		if r := recover(); r != nil {
			log.Printf("[WARN] Execute recover from panic: callId: %s table: %s error: %v", req.CallId, req.Table, r)
			log.Printf("[WARN] %s", debug.Stack())
			err = helpers.ToError(r)
			return
		}

		log.Printf("[TRACE] Execute complete callId: %s table: %s ", req.CallId, req.Table)
	}()

	// the connection property must be set already
	connectionData, ok := p.ConnectionMap[connectionName]
	if !ok {
		return fmt.Errorf("no connection data loaded for connection '%s'", connectionName)
	}

	log.Printf("[TRACE] got connection data")

	logging.LogTime("Start execute")

	queryContext := NewQueryContext(req.QueryContext, limitParam, cacheEnabled, cacheTTL)
	table, ok := connectionData.TableMap[req.Table]
	if !ok {
		return fmt.Errorf("plugin %s does not provide table %s", p.Name, req.Table)
	}

	log.Printf("[TRACE] Got query context, table: %s, cols: %v", req.Table, queryContext.Columns)

	// async approach
	// 1) call list() in a goroutine. This writes pages of items to the rowDataChan. When complete it closes the channel
	// 2) range over rowDataChan - for each item spawn a goroutine to build a row
	// 3) Build row spawns goroutines for any required hydrate functions.
	// 4) When hydrate functions are complete, apply transforms to generate column values. When row is ready, send on rowChan
	// 5) Range over rowChan - for each row, send on results stream
	log.Printf("[TRACE] Start execute span")
	ctx, executeSpan := p.startExecuteSpan(ctx, req)
	defer func() {
		log.Printf("[TRACE] End execute span")
		executeSpan.End()
	}()

	log.Printf("[TRACE] GetMatrixItem")

	// get the matrix item
	var matrixItem []map[string]interface{}
	if table.GetMatrixItem != nil {
		matrixItem = table.GetMatrixItem(ctx, connectionData.Connection)
	}

	log.Printf("[TRACE] newQueryData")

	queryData, err := p.newQueryData(req.CallId, queryContext, table, matrixItem, connectionData, executeData, outputChan)
	if err != nil {
		return err
	}

	limit := queryContext.GetLimit()

	log.Printf("[TRACE] calling fetchItemsAsync, table: %s, matrixItem: %v, limit: %d", table.Name, queryData.Matrix, limit)

	// convert qual map to trype used by cache
	cacheQualMap := queryData.Quals.ToProtoQualMap()
	// can we satisfy this request from the cache?
	if cacheEnabled {
		log.Printf("[TRACE] q: %s %p", req.CallId, p.queryCache)
		// try to fetch this data from the query cache

		result, err := p.queryCache.Get(ctx, table.Name, cacheQualMap, queryContext.Columns, limit, queryContext.CacheTTL, connectionName)
		if err != nil {
			log.Printf("[WARN] queryCacheGet returned err %s", err.Error())
		} else {

			log.Printf("[INFO] queryCacheGet returned CACHE HIT")
			queryData.streamCacheResult(result)

			// TODO maybe runtime.GC() ????

			// nothing more to do
			return nil
		}

		log.Printf("[INFO] queryCacheGet returned CACHE MISS")

		// the cache will have added a pending item for this transfer
		// and it is our responsibility to either call 'set' or 'cancel' for this pending item
		defer func() {
			if err != nil || ctx.Err() != nil {
				log.Printf("[WARN] Execute call failed err: %v cancelled: %v - cancelling pending item in cache", err, ctx.Err())
				p.queryCache.CancelPendingItem(table.Name, queryContext.UnsafeQuals, queryContext.Columns, limit, connectionName)
			}
		}()
	} else {
		log.Printf("[TRACE] Cache DISABLED callId: %s", req.CallId)
	}

	log.Printf("[TRACE] fetch items callId: %s", req.CallId)
	// asyncronously fetch items
	if err := table.fetchItemsAsync(ctx, queryData); err != nil {
		log.Printf("[WARN] fetchItemsAsync returned an error, table: %s, error: %v", table.Name, err)
		return err

	}
	logging.LogTime("Calling build Rows")

	log.Printf("[TRACE] buildRowsAsync callId: %s", req.CallId)

	// asyncronously build rows
	// channel used by streamRows when it receives an error to tell buildRowsAsync to stop
	doneChan := make(chan bool)
	queryData.buildRowsAsync(ctx, rowChan, doneChan)

	log.Printf("[TRACE] streamRows callId: %s", req.CallId)

	logging.LogTime("Calling streamRows")

	//  stream rows across GRPC
	rows, err := queryData.streamRows(ctx, rowChan, doneChan)

	if err != nil {
		return err
	}
	err = p.queryCache.Set(ctx, rows, table.Name, cacheQualMap, queryContext.Columns, limit, connectionName)
	if err != nil {
		// just log error, do not fail
		log.Printf("[WARN] cache set failed: %v", err)
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

func (p *Plugin) newQueryData(callId string, queryContext *QueryContext, table *Table, matrixItem []map[string]interface{}, connectionData *ConnectionData, executeConnectionData *proto.ExecuteConnectionData, outputChan chan *proto.ExecuteResponse) (*QueryData, error) {
	// lock access to the newQueryData - otherwise plugin crashes were observed
	p.concurrencyLock.Lock()
	defer p.concurrencyLock.Unlock()

	return newQueryData(callId, p, queryContext, table, matrixItem, connectionData, executeConnectionData, outputChan)
}

// initialiseTables does 2 things:
// 1) if a TableMapFunc factory function was provided by the plugin, call it
// 2) call initialise on the table, plassing the plugin pointer which the table stores
func (p *Plugin) initialiseTables(ctx context.Context, connection *Connection) (tableMap map[string]*Table, err error) {

	tableMap = p.TableMap

	if p.TableMapFunc != nil {
		// handle panic in factory function
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("failed to plugin initialise plugin '%s': TableMapFunc '%s' had unhandled error: %v", p.Name, helpers.GetFunctionName(p.TableMapFunc), helpers.ToError(r))
			}
		}()

		tableMap, err = p.TableMapFunc(ctx, p, connection)
		if err != nil {
			return nil, err
		}
	}

	// update tables to have a reference to the plugin
	for _, table := range tableMap {
		table.initialise(p)
	}

	// now validate the plugin
	// NOTE: must do this after calling TableMapFunc
	if validationErrors := p.Validate(); validationErrors != "" {
		return nil, fmt.Errorf("plugin %s validation failed: \n%s", p.Name, validationErrors)
	}
	return tableMap, nil
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
	// build a connection schema map
	connectionSchemaMap := p.buildConnectionSchemaMap()
	if p.queryCache == nil {
		// TODO max cache storage
		queryCache, err := query_cache.NewQueryCache(p.Name, connectionSchemaMap, 10000000)
		if err != nil {
			return err
		}
		p.queryCache = queryCache
	} else {
		// so there is already a cache - that means the config has been updated, not set for the first time

		// update the schema map on the query cache
		p.queryCache.PluginSchemaMap = connectionSchemaMap
	}

	return nil
}

func (p *Plugin) buildSchema(tableMap map[string]*Table) (map[string]*proto.TableSchema, error) {
	schema := map[string]*proto.TableSchema{}

	var tables []string
	for tableName, table := range tableMap {
		tableSchema, err := table.GetSchema()
		if err != nil {
			return nil, err
		}
		schema[tableName] = tableSchema
		tables = append(tables, tableName)
	}

	return schema, nil
}

func (p *Plugin) buildConnectionSchemaMap() map[string]*grpc.PluginSchema {
	res := make(map[string]*grpc.PluginSchema, len(p.ConnectionMap))
	for k, v := range p.ConnectionMap {
		res[k] = &grpc.PluginSchema{
			Schema: v.Schema,
			Mode:   p.SchemaMode,
		}
	}
	return res
}

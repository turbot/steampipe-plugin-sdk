package plugin

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/eko/gocache/v3/cache"
	"github.com/eko/gocache/v3/store"
	"github.com/fsnotify/fsnotify"
	"github.com/hashicorp/go-hclog"
	"github.com/turbot/go-kit/helpers"
	connectionmanager "github.com/turbot/steampipe-plugin-sdk/v5/connection"
	"github.com/turbot/steampipe-plugin-sdk/v5/error_helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"github.com/turbot/steampipe-plugin-sdk/v5/query_cache"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe-plugin-sdk/v5/version"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"
)

const (
	SchemaModeStatic  = "static"
	SchemaModeDynamic = "dynamic"
)

var validSchemaModes = []string{SchemaModeStatic, SchemaModeDynamic}

/*
Plugin is the primary struct that defines a Steampipe GRPC plugin.

Set plugin name using [plugin.Plugin.Name].

The tables provided by the plugin are specified by setting either [plugin.Plugin.TableMap] or [plugin.Plugin.TableMapFunc]:

  - For most plugins, with a static set of tables, use [plugin.Plugin.TableMap].

  - For a plugin with [dynamic_tables], use [plugin.Plugin.TableMapFunc]. Also, [plugin.Plugin.SchemaMode] must be set to dynamic.

If the plugin uses custom connection config, it must define a [plugin.ConnectionConfigSchema].

Various default behaviours can be defined:

  - A default [transform] which will be applied to all columns which do not specify a transform. ([plugin.Plugin.DefaultTransform]).

  - The default concurrency limits for a [HydrateFunc] ([plugin.Plugin.DefaultConcurrency]).

  - The plugin default [error_handling] behaviour.

Required columns can be specified by setting [plugin.Plugin.RequiredColumns].

Plugin examples:
  - [aws]
  - [github]
  - [hackernews]

[aws]: https://github.com/turbot/steampipe-plugin-aws/blob/c5fbf38df19667f60877c860cf8ad39816ff658f/aws/plugin.go#L19
[github]: https://github.com/turbot/steampipe-plugin-github/blob/a5ae211ee602be4adcea3a5c495cbe36aa87b957/github/plugin.go#L11
[hackernews]: https://github.com/turbot/steampipe-plugin-hackernews/blob/bbfbb12751ad43a2ca0ab70901cde6a88e92cf44/hackernews/plugin.go#L10
*/
type Plugin struct {
	Name   string
	Logger hclog.Logger
	// TableMap is a map of all the tables in the plugin, keyed by the table name
	// NOTE: it must be NULL for plugins with dynamic schema
	TableMap            map[string]*Table
	TableMapFunc        TableMapFunc
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
	// ConnectionConfigChangedFunc is a callback function which is called from UpdateConnectionConfigs
	// when any connection configs have changed
	ConnectionConfigChangedFunc func(ctx context.Context, p *Plugin, old, new *Connection) error

	// map of connection data (schema, config, connection cache)
	// keyed by connection name
	ConnectionMap map[string]*ConnectionData
	// is this a static or dynamic schema
	SchemaMode string
	// callback function which is called when any watched source file(s) gets changed
	WatchedFileChangedFunc func(ctx context.Context, p *Plugin, connection *Connection, events []fsnotify.Event)

	queryCache *query_cache.QueryCache
	// shared connection cache - this is the underlying cache used for all queryData ConnectionCache
	connectionCacheStore *cache.Cache[any]
	// map of the connection caches, keyed by connection name
	connectionCacheMap     map[string]*connectionmanager.ConnectionCache
	connectionCacheMapLock sync.Mutex

	// temporary dir for this plugin
	// this will only created if GetSourceFiles is used
	tempDir string
	// stream used to send messages back to plugin manager
	messageStream proto.WrapperPlugin_EstablishMessageStreamServer
}

// initialise creates the 'connection manager' (which provides caching), sets up the logger
// and sets the file limit.
func (p *Plugin) initialise() {
	p.ConnectionMap = make(map[string]*ConnectionData)
	p.connectionCacheMap = make(map[string]*connectionmanager.ConnectionCache)

	p.Logger = p.setupLogger()
	log.Printf("[INFO] initialise plugin '%s', using sdk version %s", p.Name, version.String())

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

	// create default ConnectionConfigChangedFunc if needed
	if p.ConnectionConfigChangedFunc == nil {
		p.ConnectionConfigChangedFunc = defaultConnectionConfigChangedFunc
	}

	// create the default WatchedFileChangedFunc if needed
	if p.WatchedFileChangedFunc == nil {
		log.Printf("[TRACE] intialising defaultWatchedFilesChangedFunc")
		p.WatchedFileChangedFunc = defaultWatchedFilesChangedFunc
	}

	if err := p.createConnectionCacheStore(); err != nil {
		panic(fmt.Sprintf("failed to create connection cache: %s", err.Error()))
	}

	// set temporary dir for this plugin
	// this will only created if GetSourceFiles is used
	p.tempDir = path.Join(os.TempDir(), p.Name)
}

func (p *Plugin) shutdown() {
	// iterate through the connections in the plugin and
	// stop the file watchers for each
	for _, connectionData := range p.ConnectionMap {
		if watcher := connectionData.Watcher; watcher != nil {
			watcher.Close()
		}
	}

	// destroy the temp directory
	err := os.RemoveAll(p.tempDir)
	if err != nil {
		log.Printf("[WARN] failed to delete the temp directory %s: %s", p.tempDir, err.Error())
	}
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

func (p *Plugin) ensureConnectionCache(connectionName string) *connectionmanager.ConnectionCache {
	connectionCache := connectionmanager.NewConnectionCache(connectionName, p.connectionCacheStore)
	p.connectionCacheMapLock.Lock()
	defer p.connectionCacheMapLock.Unlock()
	// add to map of connection caches
	p.connectionCacheMap[connectionName] = connectionCache
	return connectionCache
}

/*
EstablishMessageStream establishes a streaming message connection between the plugin and the plugin manager
This is used if the plugin has a dynamic schema and uses file watching

This is the handler function for the EstablishMessageStream grpc function
*/
func (p *Plugin) EstablishMessageStream(stream proto.WrapperPlugin_EstablishMessageStreamServer) error {
	log.Printf("[TRACE] plugin.EstablishMessageStream plugin %p, stream %p", p, stream)
	// if the plugin does not have a dynamic schema, we do not need the message stream
	if p.SchemaMode != SchemaModeDynamic {
		log.Printf("[TRACE] EstablishMessageStream - plugin %s has static schema so no message stream, required", p.Name)
		return nil
	}

	p.messageStream = stream

	log.Printf("[TRACE] plugin.EstablishMessageStream set on plugin: plugin.messageStream %p", p.messageStream)

	// hold stream open
	for {
	}
	return nil
}

/*
GetSchema returns the [grpc.PluginSchema].
Note: the connection config must be set before calling this function.

This is the handler function for the GetSchema grpc function
*/
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

// Execute starts a query and streams the results using the given GRPC stream.
//
// This is the handler function for the Execute GRPC function.
func (p *Plugin) Execute(req *proto.ExecuteRequest, stream proto.WrapperPlugin_ExecuteServer) (err error) {
	// add CallId to logs for the execute call
	logger := p.Logger.Named(req.CallId)
	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)

	log.Printf("[INFO] Plugin Execute (%s)", req.CallId)
	defer log.Printf("[INFO]  Plugin Execute complete (%s)", req.CallId)

	// limit the plugin memory
	newLimit := GetMaxMemoryBytes()
	debug.SetMemoryLimit(newLimit)
	log.Printf("[INFO] Plugin Execute, setting memory limit to %dMb", newLimit/(1024*1024))

	outputChan := make(chan *proto.ExecuteResponse, len(req.ExecuteConnectionData))
	errorChan := make(chan error, len(req.ExecuteConnectionData))
	//doneChan := make(chan bool)
	var outputWg sync.WaitGroup

	// get a context which includes telemetry data and logger
	ctx := p.buildExecuteContext(stream.Context(), req, logger)

	// control how many connections are executed in parallel
	maxConcurrentConnections := getMaxConcurrentConnections()
	sem := semaphore.NewWeighted(int64(maxConcurrentConnections))

	for connectionName, executeData := range req.ExecuteConnectionData {
		outputWg.Add(1)

		go func(c string) {
			defer outputWg.Done()

			if err := sem.Acquire(ctx, 1); err != nil {
				return
			}
			defer sem.Release(1)

			if err := p.executeForConnection(ctx, req, c, executeData, outputChan); err != nil {
				if !error_helpers.IsContextCancelledError(err) {
					log.Printf("[WARN] executeForConnection %s returned error %s", c, err.Error())
				}
				errorChan <- err
			}
			log.Printf("[TRACE] executeForConnection %s returned", c)
		}(connectionName)
	}

	var errors []error

	go func() {
		outputWg.Wait()
		// so all executeForConnection calls are complete
		// stream a nil row to indicate completion
		log.Printf("[TRACE] output wg complete - send nil row")
		outputChan <- nil
	}()

	complete := false
	for !complete {
		select {
		case row := <-outputChan:
			// nil row means that one connection is done streaming
			if row == nil {
				log.Printf("[TRACE] empty row on output channel - we are done ")
				complete = true
				break
			}

			if err := stream.Send(row); err != nil {
				// ignore context cancellation - they will get picked up further downstream
				if !error_helpers.IsContextCancelledError(err) {
					errors = append(errors, grpc.HandleGrpcError(err, p.Name, "stream.Send"))
				}
				break
			}
		case err := <-errorChan:
			if !error_helpers.IsContextCancelledError(err) {
				log.Printf("[WARN] error channel received %s", err.Error())
			}
			errors = append(errors, err)
		}
	}

	close(outputChan)
	close(errorChan)

	return helpers.CombineErrors(errors...)
}

// ClearConnectionCache clears the connection cache for the given connection.
func (p *Plugin) ClearConnectionCache(ctx context.Context, connectionName string) {
	p.connectionCacheMapLock.Lock()
	defer p.connectionCacheMapLock.Unlock()

	// get the connection cache for this connection
	connectionCache, ok := p.connectionCacheMap[connectionName]
	if !ok {
		// not expected
		log.Printf("[TRACE] ClearConnectionCache failed - no connection cache found for connection %s", connectionName)
		return
	}
	connectionCache.Clear(ctx)
}

// ClearQueryCache clears the query cache for the given connection.
func (p *Plugin) ClearQueryCache(ctx context.Context, connectionName string) {
	p.queryCache.ClearForConnection(ctx, connectionName)
}

// ConnectionSchemaChanged sends a message to the plugin-manager that the schema of this plugin has changed
//
// This should be called from the plugin implementation of [plugin.Plugin.WatchedFileChangedFunc]
// if a change in watched source files has changed the plugin schema.
func (p *Plugin) ConnectionSchemaChanged(connection *Connection) error {
	log.Printf("[TRACE] ConnectionSchemaChanged plugin %s, connection %s", p.Name, connection.Name)

	// get the updated table map and schema
	tableMap, schema, err := p.refreshSchema(connection)
	if err != nil {
		return err
	}
	// update the connection data
	p.ConnectionMap[connection.Name] = &ConnectionData{
		TableMap:   tableMap,
		Connection: connection,
		Schema:     schema,
		Plugin:     p,
	}

	// let the plugin manager know
	if p.messageStream != nil {
		return p.messageStream.Send(&proto.PluginMessage{
			MessageType: proto.PluginMessageType_SCHEMA_UPDATED,
			Connection:  connection.Name,
		})
	}
	return nil
}

func (p *Plugin) executeForConnection(ctx context.Context, req *proto.ExecuteRequest, connectionName string, executeData *proto.ExecuteConnectionData, outputChan chan *proto.ExecuteResponse) (err error) {
	const rowBufferSize = 10
	var rowChan = make(chan *proto.Row, rowBufferSize)

	// build callId for this connection (this is necessary is the plugin Execute call may be for an aggregator connection)
	connectionCallId := grpc.BuildConnectionCallId(req.CallId, connectionName)
	log.Printf("[TRACE] executeForConnection callId: %s, connectionCallId: %s, connection: %s table: %s cols: %s", req.CallId, connectionCallId, connectionName, req.Table, strings.Join(req.QueryContext.Columns, ","))

	defer func() {
		log.Printf("[TRACE] executeForConnection DEFER (%s) ", connectionCallId)
		if r := recover(); r != nil {
			log.Printf("[WARN] Execute recover from panic: callId: %s table: %s error: %v", connectionCallId, req.Table, r)
			err = helpers.ToError(r)
			return
		}

		log.Printf("[TRACE] Execute complete callId: %s table: %s ", connectionCallId, req.Table)
	}()

	// the connection property must be set already
	connectionData, ok := p.ConnectionMap[connectionName]
	if !ok {
		return fmt.Errorf("plugin execute failed - no connection data loaded for connection '%s'", connectionName)
	}
	log.Printf("[TRACE] got connection data")

	table, ok := connectionData.TableMap[req.Table]
	if !ok {
		return fmt.Errorf("plugin %s does not provide table %s", p.Name, req.Table)
	}

	// get limit and cache vars
	limitParam := executeData.Limit
	cacheTTL := executeData.CacheTtl
	cacheEnabled := executeData.CacheEnabled

	// check whether the cache is disabled for this table
	if table.Cache != nil {
		cacheEnabled = table.Cache.Enabled && cacheEnabled
		if !cacheEnabled {
			log.Printf("[INFO] caching is disabled for table %s", table.Name)
		}
	}

	logging.LogTime("Start execute")

	queryContext := NewQueryContext(req.QueryContext, limitParam, cacheEnabled, cacheTTL)

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

	queryData, err := newQueryData(connectionCallId, p, queryContext, table, connectionData, executeData, outputChan)
	if err != nil {
		return err
	}

	// get the matrix item
	log.Printf("[TRACE] GetMatrixItem")
	var matrixItem []map[string]interface{}
	if table.GetMatrixItem != nil {
		matrixItem = table.GetMatrixItem(ctx, connectionData.Connection)
	}
	if table.GetMatrixItemFunc != nil {
		matrixItem = table.GetMatrixItemFunc(ctx, queryData)
	}
	queryData.setMatrixItem(matrixItem)

	log.Printf("[TRACE] creating query data")

	limit := queryContext.GetLimit()

	// convert qual map to type used by cache
	cacheQualMap := queryData.getCacheQualMap()
	// build cache request
	cacheRequest := &query_cache.CacheRequest{
		Table:          table.Name,
		QualMap:        cacheQualMap,
		Columns:        queryContext.Columns,
		Limit:          limit,
		ConnectionName: connectionName,
		TtlSeconds:     queryContext.CacheTTL,
		CallId:         connectionCallId,
	}
	// can we satisfy this request from the cache?
	if cacheEnabled {
		log.Printf("[INFO] cacheEnabled, trying cache get (%s)", connectionCallId)

		// create a function to increment cachedRowsFetched and stream a row
		streamRowFunc := func(row *proto.Row) {
			// if row is not nil (indicating completion), increment cachedRowsFetched
			if row != nil {
				atomic.AddInt64(&queryData.queryStatus.cachedRowsFetched, 1)
			}
			queryData.streamRow(row)
		}

		start := time.Now()
		// try to fetch this data from the query cache
		cacheErr := p.queryCache.Get(ctx, cacheRequest, streamRowFunc)
		if cacheErr == nil {
			// so we got a cached result - stream it out
			log.Printf("[INFO] queryCacheGet returned CACHE HIT (%s)", connectionCallId)

			// nothing more to do
			return nil
		}

		getDuration := time.Since(start)
		if getDuration > time.Second {
			log.Printf("[TRACE] queryCache.Get took %.1fs: (%s),", getDuration.Seconds(), connectionCallId)
		}

		// so the cache call failed, with either a cache-miss or other error
		if query_cache.IsCacheMiss(cacheErr) {
			log.Printf("[TRACE] cache MISS")
		} else if errors.Is(cacheErr, error_helpers.QueryError{}) {
			// if this is a QueryError, this means the pending item we were waitign for failed
			// > we also fail
			return cacheErr
		} else {
			// otherwise just log the cache error
			log.Printf("[TRACE] queryCacheGet returned err %s", cacheErr.Error())
		}

		log.Printf("[INFO] queryCacheGet returned CACHE MISS (%s)", connectionCallId)
		p.queryCache.StartSet(ctx, cacheRequest)
	} else {
		log.Printf("[INFO] Cache DISABLED connectionCallId: %s", connectionCallId)
	}

	// asyncronously fetch items
	log.Printf("[TRACE] calling fetchItems, table: %s, matrixItem: %v, limit: %d,  connectionCallId: %s\"", table.Name, queryData.Matrix, limit, connectionCallId)
	if err := table.fetchItems(ctx, queryData); err != nil {
		log.Printf("[WARN] fetchItems returned an error, table: %s, error: %v", table.Name, err)
		return err

	}
	logging.LogTime("Calling build Rows")

	log.Printf("[TRACE] buildRowsAsync connectionCallId: %s", connectionCallId)

	// asyncronously build rows
	// channel used by streamRows when it receives an error to tell buildRowsAsync to stop
	doneChan := make(chan bool)
	queryData.buildRowsAsync(ctx, rowChan, doneChan)

	log.Printf("[TRACE] streamRows connectionCallId: %s", connectionCallId)

	logging.LogTime("Calling streamRows")

	//  stream rows across GRPC
	err = queryData.streamRows(ctx, rowChan, doneChan)
	if err != nil {
		return err
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

		tableMapData := &TableMapData{
			Connection:     connection,
			ConectionCache: p.ensureConnectionCache(connection.Name),
		}
		tableMap, err = p.TableMapFunc(ctx, tableMapData)
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
	if validationErrors := p.validate(); validationErrors != "" {
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

// if query cache does not exist, create
// if the query cache exists, update the schema
func (p *Plugin) ensureCache(maxCacheSizeMb int) error {
	// build a connection schema map
	connectionSchemaMap := p.buildConnectionSchemaMap()
	if p.queryCache == nil {
		log.Printf("[TRACE] Plugin ensureCache creating cache, maxCacheStorageMb %d", maxCacheSizeMb)

		queryCache, err := query_cache.NewQueryCache(p.Name, connectionSchemaMap, maxCacheSizeMb)
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

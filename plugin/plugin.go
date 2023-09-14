package plugin

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/eko/gocache/v3/cache"
	"github.com/eko/gocache/v3/store"
	"github.com/fsnotify/fsnotify"
	"github.com/gertd/go-pluralize"
	"github.com/hashicorp/go-hclog"
	"github.com/turbot/go-kit/helpers"
	connectionmanager "github.com/turbot/steampipe-plugin-sdk/v5/connection"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"github.com/turbot/steampipe-plugin-sdk/v5/query_cache"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe-plugin-sdk/v5/version"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

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
	TableMap         map[string]*Table
	TableMapFunc     TableMapFunc
	DefaultTransform *transform.ColumnTransforms
	// deprecated - use RateLimiters to control concurrency
	DefaultConcurrency  *DefaultConcurrencyConfig
	DefaultRetryConfig  *RetryConfig
	DefaultIgnoreConfig *IgnoreConfig

	// rate limiter definitions - these are (optionally) defined by the plugin author
	// and do NOT include any config overrides
	RateLimiters []*rate_limiter.Definition

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

	// config for hydrate functions which are used across multiple tables
	HydrateConfig []HydrateConfig

	queryCache *query_cache.QueryCache
	// shared connection cache - this is the underlying cache used for all queryData ConnectionCache
	connectionCacheStore *cache.Cache[any]
	// map of the connection caches, keyed by connection name
	connectionCacheMap     map[string]*connectionmanager.ConnectionCache
	connectionCacheMapLock sync.Mutex

	// temporary dir for this plugin
	// this will only created if getSourceFiles is used
	tempDir string
	// stream used to send messages back to plugin manager
	messageStream proto.WrapperPlugin_EstablishMessageStreamServer

	// map of rate limiter INSTANCES - these are lazy loaded
	// keyed by stringified scope values
	rateLimiterInstances *rate_limiter.LimiterMap
	// map of rate limiter definitions, keyed by limiter name
	// NOTE: this includes limiters defined/overridden in config
	resolvedRateLimiterDefs map[string]*rate_limiter.Definition
	// lock for this map
	rateLimiterDefsMut sync.RWMutex

	// map of call ids to avoid duplicates
	callIdLookup    map[string]struct{}
	callIdLookupMut sync.RWMutex

	// map of hydrate function name to columns it provides
	hydrateConfigMap map[string]*HydrateConfig
}

// initialise creates the 'connection manager' (which provides caching), sets up the logger
// and sets the file limit.
func (p *Plugin) initialise(logger hclog.Logger) {

	log.Printf("[WARN] initialise")

	//time.Sleep(10 * time.Second)
	p.ConnectionMap = make(map[string]*ConnectionData)
	p.connectionCacheMap = make(map[string]*connectionmanager.ConnectionCache)

	p.Logger = logger
	log.Printf("[INFO] initialise plugin '%s', using sdk version %s", p.Name, version.String())

	p.initialiseRateLimits()

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
	// this will only created if getSourceFiles is used
	p.tempDir = path.Join(os.TempDir(), p.Name)

	p.callIdLookup = make(map[string]struct{})
}

func (p *Plugin) initialiseRateLimits() {
	p.rateLimiterInstances = rate_limiter.NewLimiterMap()
	p.populatePluginRateLimiters()
	return
}

// populate resolvedRateLimiterDefs map with plugin rate limiter definitions
func (p *Plugin) populatePluginRateLimiters() {
	p.resolvedRateLimiterDefs = make(map[string]*rate_limiter.Definition, len(p.RateLimiters))
	for _, d := range p.RateLimiters {
		// NOTE: we have not validated the limiter definitions yet
		// (this is done from initialiseTables, after setting the connection config),
		// so just ignore limiters with no name (validation will fail later if this occurs)
		if d.Name != "" {
			p.resolvedRateLimiterDefs[d.Name] = d
		}
	}
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
	if p.queryCache.Enabled {
		p.queryCache.ClearForConnection(ctx, connectionName)
	}
}

// ConnectionSchemaChanged sends a message to the plugin-manager that the schema of this plugin has changed
//
// This should be called from the plugin implementation of [plugin.Plugin.WatchedFileChangedFunc]
// if a change in watched source files has changed the plugin schema.
func (p *Plugin) ConnectionSchemaChanged(connection *Connection) error {
	log.Printf("[TRACE] ConnectionSchemaChanged plugin %s, connection %s", p.Name, connection.Name)

	oldSchema := p.ConnectionMap[connection.Name].Schema

	// get the updated table map and schema
	tableMap, schema, err := p.getConnectionSchema(connection)
	if err != nil {
		return err
	}
	// update the connection data
	p.ConnectionMap[connection.Name].setSchema(tableMap, schema)

	// if there are changes,  let the plugin manager know
	if !oldSchema.Equals(schema) && p.messageStream != nil {
		return p.messageStream.Send(&proto.PluginMessage{
			MessageType: proto.PluginMessageType_SCHEMA_UPDATED,
			Connection:  connection.Name,
		})
	}
	return nil
}

func (p *Plugin) executeForConnection(streamContext context.Context, req *proto.ExecuteRequest, connectionName string, outputChan chan *proto.ExecuteResponse, logger hclog.Logger) (err error) {
	const rowBufferSize = 10
	var rowChan = make(chan *proto.Row, rowBufferSize)

	executeData := req.ExecuteConnectionData[connectionName]

	// build callId for this connection (this is necessary is the plugin Execute call may be for an aggregator connection)
	connectionCallId := p.getConnectionCallId(req.CallId, connectionName)

	log.Printf("[INFO] executeForConnection callId: %s, connectionCallId: %s, connection: %s table: %s cols: %s", req.CallId, connectionCallId, connectionName, req.Table, strings.Join(req.QueryContext.Columns, ","))

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] executeForConnection recover from panic: callId: %s table: %s error: %v", connectionCallId, req.Table, r)
			err = helpers.ToError(r)
			return
		}
		log.Printf("[INFO] executeForConnection COMPLETE callId: %s, connectionCallId: %s, connection: %s table: %s cols: %s ", req.CallId, connectionCallId, connectionName, req.Table, strings.Join(req.QueryContext.Columns, ","))
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
	cacheEnabled := p.queryCache.Enabled && executeData.CacheEnabled

	// check whether the cache is disabled for this table
	if table.Cache != nil {
		cacheEnabled = table.Cache.Enabled && cacheEnabled
		if !cacheEnabled {
			log.Printf("[INFO] caching is disabled for table %s", table.Name)
		}
	}

	//  if cache NOT disabled, create a fresh context for this scan
	ctx := streamContext
	var cancel context.CancelFunc
	if cacheEnabled {
		// get a fresh context which includes telemetry data and logger
		ctx, cancel = context.WithCancel(context.Background())
	}
	ctx = p.buildExecuteContext(ctx, req, logger)

	logging.LogTime("Start execute")

	queryContext := NewQueryContext(req.QueryContext, limitParam, cacheEnabled, cacheTTL, table)

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

	// set the cancel func on the query data
	// (this is only used if the cache is enabled - if a set request has no subscribers)
	queryData.cancel = cancel

	// get the matrix item
	log.Printf("[TRACE] GetMatrixItem")
	var matrixItem []map[string]any
	if table.GetMatrixItem != nil {
		matrixItem = table.GetMatrixItem(ctx, connectionData.Connection)
	}
	if table.GetMatrixItemFunc != nil {
		matrixItem = table.GetMatrixItemFunc(ctx, queryData)
	}
	queryData.setMatrix(matrixItem)

	log.Printf("[TRACE] creating query data")

	limit := queryContext.GetLimit()

	// convert qual map to type used by cache
	cacheQualMap := queryData.getCacheQualMap()
	// build cache request
	cacheRequest := &query_cache.CacheRequest{
		Table:          table.Name,
		QualMap:        cacheQualMap,
		Columns:        queryData.getColumnNames(), // all column names returned by the required hydrate functions
		Limit:          limit,
		ConnectionName: connectionName,
		TtlSeconds:     queryContext.CacheTTL,
		CallId:         connectionCallId,
		StreamContext:  streamContext,
	}
	// can we satisfy this request from the cache?
	if cacheEnabled {
		log.Printf("[INFO] cacheEnabled, trying cache get (%s)", connectionCallId)

		// create a function to increment cachedRowsFetched and stream a row
		streamUncachedRowFunc := queryData.streamRow
		streamCachedRowFunc := func(row *proto.Row) {
			// if row is not nil (indicating completion), increment cachedRowsFetched
			if row != nil {
				atomic.AddInt64(&queryData.queryStatus.cachedRowsFetched, 1)
			}
			streamUncachedRowFunc(row)
		}

		start := time.Now()
		// try to fetch this data from the query cache
		cacheErr := p.queryCache.Get(ctx, cacheRequest, streamUncachedRowFunc, streamCachedRowFunc)
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
		if !query_cache.IsCacheMiss(cacheErr) {
			log.Printf("[WARN] queryCacheGet returned err %s", cacheErr.Error())
			return cacheErr
		}
		// otherwise just log the cache miss error
		log.Printf("[INFO] queryCacheGet returned CACHE MISS (%s)", connectionCallId)
	} else {
		log.Printf("[INFO] Cache DISABLED (%s)", connectionCallId)
	}

	// so we need to fetch the data

	// asyncronously fetch items
	log.Printf("[INFO] calling fetchItems, table: %s, matrixItem: %v, limit: %d  (%s)", table.Name, queryData.Matrix, limit, connectionCallId)
	if err := table.fetchItems(ctx, queryData); err != nil {
		log.Printf("[WARN] fetchItems returned an error, table: %s, error: %v", table.Name, err)
		return err

	}

	// asyncronously build rows
	logging.LogTime("Calling build Rows")
	log.Printf("[TRACE] buildRowsAsync (%s)", connectionCallId)

	// channel used by streamRows when it receives an error to tell buildRowsAsync to stop
	doneChan := make(chan bool)
	queryData.buildRowsAsync(ctx, rowChan, doneChan)

	//  stream rows either into cache (if enabled) or back across GRPC (if not)
	logging.LogTime("Calling streamRows")

	err = queryData.streamRows(ctx, rowChan, doneChan)
	if err != nil {
		log.Printf("[WARN] queryData.streamRows returned error: %s", err.Error())
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

// initialiseTables does 3 things:
// 1) if a TableMapFunc factory function was provided by the plugin, call it
// 2) call initialise on the table, passing the plugin pointer which the table stores
// 3) validate the plugin
func (p *Plugin) initialiseTables(ctx context.Context, connection *Connection) (tableMap map[string]*Table, err error) {
	log.Printf("[TRACE] Plugin %s initialiseTables", p.Name)

	tableMap = p.TableMap

	if p.TableMapFunc != nil {
		// handle panic in factory function
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("failed to plugin initialise plugin '%s': TableMapFunc '%s' had unhandled error: %v", p.Name, helpers.GetFunctionName(p.TableMapFunc), helpers.ToError(r))
			}
		}()

		tableMapData := &TableMapData{
			Connection:      connection,
			ConnectionCache: p.ensureConnectionCache(connection.Name),
			tempDir:         getConnectionTempDir(p.tempDir, connection.Name),
		}
		tableMap, err = p.TableMapFunc(ctx, tableMapData)
		if err != nil {
			return nil, err
		}
	}

	// populate map of global hydrate config
	p.buildHydrateConfigMap()

	// initialise all tables
	for _, table := range tableMap {
		table.initialise(p)
	}

	// NOW finally validate the plugin
	// NOTE: must do this after calling TableMapFunc
	validationWarnings, validationErrors := p.validate(tableMap)

	if len(validationWarnings) > 0 {
		logValidationWarning(connection, validationWarnings)
	}
	if len(validationErrors) > 0 {
		return nil, fmt.Errorf("plugin %s connection %s validation failed: \n%s", p.Name, connection.Name, strings.Join(validationErrors, "\n"))
	}
	return tableMap, nil
}

// build map of all hydrate configs, including those specified in the legacy HydrateDependencies,
// and those mentioned only in column config
func (p *Plugin) buildHydrateConfigMap() {
	p.hydrateConfigMap = make(map[string]*HydrateConfig)
	for i := range p.HydrateConfig {
		// as we are converting into a pointer, we cannot use the array value direct from the range as
		// this was causing incorrect values - go must be reusing memory addresses for successive items
		h := &p.HydrateConfig[i]
		// initialise before adding to map
		h.initialise(nil)
		funcName := h.namedFunc.Name
		p.hydrateConfigMap[funcName] = h
	}
}

func logValidationWarning(connection *Connection, warnings []string) {
	count := len(warnings)
	log.Printf("[WARN] connection %s, has %d table validation %s",
		connection.Name,
		count,
		pluralize.NewClient().Pluralize("warning", count, false))

	for _, w := range warnings {
		log.Printf("[WARN] %s", w)
	}
}

// if query cache does not exist, create
// if the query cache exists, update the schema
func (p *Plugin) ensureCache(connectionSchemaMap map[string]*grpc.PluginSchema, opts *query_cache.QueryCacheOptions) error {
	log.Printf("[TRACE] Plugin ensureCache creating cache, maxCacheStorageMb %d", opts.MaxSizeMb)

	queryCache, err := query_cache.NewQueryCache(p.Name, connectionSchemaMap, opts)
	if err != nil {
		return err
	}
	p.queryCache = queryCache
	return nil
}

func (p *Plugin) buildSchema(tableMap map[string]*Table) (*grpc.PluginSchema, error) {
	schema := grpc.NewPluginSchema(p.SchemaMode)

	var tables []string
	for tableName, table := range tableMap {
		tableSchema, err := table.GetSchema()
		if err != nil {
			return nil, err
		}
		schema.Schema[tableName] = tableSchema
		tables = append(tables, tableName)
	}

	return schema, nil
}

func (p *Plugin) buildConnectionSchemaMap() map[string]*grpc.PluginSchema {
	res := make(map[string]*grpc.PluginSchema, len(p.ConnectionMap))
	for k, v := range p.ConnectionMap {
		res[k] = v.Schema
	}
	return res
}

// ensure callId is unique fo rthis plugin instance - important as it is used to key set requests
func (p *Plugin) getUniqueCallId(callId string) string {
	// store as orig as we may mutate connectionCallId to dedupe
	orig := callId
	// check if it unique - this is crucial as it is used to key 'set requests` in the query cache
	idx := 0
	p.callIdLookupMut.RLock()
	for {
		if _, callIdExists := p.callIdLookup[callId]; !callIdExists {
			// release read lock and get a write lock
			p.callIdLookupMut.RUnlock()
			p.callIdLookupMut.Lock()

			// recheck as ther eis a race condition to acquire a write lockm
			if _, callIdExists := p.callIdLookup[callId]; !callIdExists {
				// store in map
				p.callIdLookup[callId] = struct{}{}
				p.callIdLookupMut.Unlock()
				return callId
			}

			// someone must have got in there before us - downgrade lock again
			p.callIdLookupMut.Unlock()
			p.callIdLookupMut.RLock()
		}
		// so the id exists already - add a suffix
		log.Printf("[WARN] getUniqueCallId duplicate call id %s - adding suffix", callId)
		callId = fmt.Sprintf("%s%d", orig, idx)
		idx++

	}
	p.callIdLookupMut.RUnlock()
	return callId
}

func (p *Plugin) getConnectionCallId(callId string, connectionName string) string {
	// add connection name onto call id
	return grpc.BuildConnectionCallId(callId, connectionName)
}

// remove callId from callIdLookup
func (p *Plugin) clearCallId(connectionCallId string) {
	p.callIdLookupMut.Lock()
	delete(p.callIdLookup, connectionCallId)
	p.callIdLookupMut.Unlock()
}

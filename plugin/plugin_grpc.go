package plugin

import (
	"context"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/error_helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/v5/query_cache"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/semaphore"
	"log"
	"runtime/debug"
	"sync"
)

/*
setConnectionConfig sets the connection config for the given connection.
(for legacy plugins)
This is the handler function for the setConnectionConfig GRPC function.
*/
func (p *Plugin) setConnectionConfig(connectionName, connectionConfigString string) (err error) {
	log.Printf("[TRACE] setConnectionConfig %s", connectionName)
	failedConnections, err := p.setAllConnectionConfigs([]*proto.ConnectionConfig{
		{
			Connection: connectionName,
			Config:     connectionConfigString,
		},
	}, 0)
	if err != nil {
		return err
	}
	if len(failedConnections) > 0 {
		return failedConnections[connectionName]
	}
	return nil
}

/*
setAllConnectionConfigs sets the connection config for a list of connections.

This is the handler function for the setAllConnectionConfigs GRPC function.
*/
func (p *Plugin) setAllConnectionConfigs(configs []*proto.ConnectionConfig, maxCacheSizeMb int) (_ map[string]error, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("setAllConnectionConfigs failed: %s", helpers.ToError(r).Error())
		} else {
			p.Logger.Debug("setAllConnectionConfigs finished")
		}
	}()
	// create a struct to populate with exemplar schema and connection failures
	// this will be passed into update functions and may be mutated
	updateData := NewConnectionUpdateData()
	p.addConnections(configs, updateData)

	// TODO report log messages back somewhere
	_, err = p.setAggregatorSchemas()
	if err != nil {
		return updateData.failedConnections, err
	}

	// if the version of the CLI does not support SetCacheOptions,
	// it will pass the max size as part of SetAllConnectionConfigs
	// (if the CLI _does_ support SetCacheOptions it will pass -1 as maxCacheSize)
	if maxCacheSizeMb != -1 {
		// build a connection schema map - used to pass to cache
		connectionSchemaMap := p.buildConnectionSchemaMap()
		// now create the query cache - do this AFTER setting the connection config so we can pass the connection schema map
		opts := &query_cache.QueryCacheOptions{
			Enabled:   true,
			Ttl:       query_cache.DefaultMaxTtl,
			MaxSizeMb: maxCacheSizeMb,
		}
		err = p.ensureCache(connectionSchemaMap, opts)
		if err != nil {
			return updateData.failedConnections, err
		}
	}

	// if there are any failed connections, raise an error
	err = error_helpers.CombineErrors(maps.Values(updateData.failedConnections)...)
	return updateData.failedConnections, err
}

/*
updateConnectionConfigs handles added, changed and deleted connections:

  - Added connections are inserted into [plugin.Plugin.ConnectionMap].

  - Deleted connections are removed from ConnectionMap.

  - For updated connections, ConnectionMap is updated and [plugin.Plugin.ConnectionConfigChangedFunc] is called.

This is the handler function for the updateConnectionConfigs GRPC function.
*/
func (p *Plugin) updateConnectionConfigs(added []*proto.ConnectionConfig, deleted []*proto.ConnectionConfig, changed []*proto.ConnectionConfig) (map[string]error, error) {
	ctx := context.WithValue(context.Background(), context_key.Logger, p.Logger)

	p.logChanges(added, deleted, changed)

	// create a struct to populate with exemplar schema and connection failures
	// this will be passed into update functions and may be mutated
	updateData := NewConnectionUpdateData()

	// if this plugin does not have dynamic config, we can share table map and schema
	if p.SchemaMode == SchemaModeStatic {
		for _, connectionData := range p.ConnectionMap {
			updateData.exemplarSchema = connectionData.Schema
			updateData.exemplarTableMap = connectionData.TableMap
			// just take the first item
			break
		}
	}

	// remove deleted connections and remove cache
	for _, deletedConnection := range deleted {
		// delete connection
		delete(p.ConnectionMap, deletedConnection.Connection)
		// delete cache wrapper struct
		delete(p.connectionCacheMap, deletedConnection.Connection)
	}

	// add added connections
	p.addConnections(added, updateData)

	// update changed connections
	// build map of current connection data for each changed connection
	p.updateConnections(ctx, changed, updateData)

	// if there are any added or changed connections, we need to rebuild all aggregator schemas
	if len(added)+len(deleted)+len(changed) > 0 {
		_, err := p.setAggregatorSchemas()
		if err != nil {
			return updateData.failedConnections, err
		}
	}

	// update the query cache schema map
	if p.queryCache.Enabled {
		p.queryCache.PluginSchemaMap = p.buildConnectionSchemaMap()
	}

	return updateData.failedConnections, nil
}

/*
getSchema returns the [grpc.PluginSchema].
Note: the connection config must be set before calling this function.

This is the handler function for the getSchema grpc function
*/
func (p *Plugin) getSchema(connectionName string) (*grpc.PluginSchema, error) {
	var connectionData *ConnectionData
	if connectionName == "" {
		// TACTICAL
		// previous steampipe versions do not pass a connection name
		// and instantiate a plugin per connection,
		// is we have more than one connection, this is an error
		if len(p.ConnectionMap) > 1 {
			return nil, fmt.Errorf("Plugin.getSchema failed - no connection name passed and multiple connections loaded")
		}
		// get first (and only) connection data
		for _, connectionData = range p.ConnectionMap {
		}
	} else {
		var ok bool
		connectionData, ok = p.ConnectionMap[connectionName]
		if !ok {
			return nil, fmt.Errorf("Plugin.getSchema failed - no connection data loaded for connection '%s'", connectionName)
		}
	}

	return connectionData.Schema, nil
}

// execute starts a query and streams the results using the given GRPC stream.
//
// This is the handler function for the execute GRPC function.
func (p *Plugin) execute(req *proto.ExecuteRequest, stream proto.WrapperPlugin_ExecuteServer) (err error) {
	// add CallId to logs for the execute call
	logger := p.Logger.Named(req.CallId)
	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)

	log.Printf("[INFO] Plugin execute table: %s  (%s)", req.Table, req.CallId)
	defer log.Printf("[INFO]  Plugin execute complete (%s)", req.CallId)

	// limit the plugin memory
	newLimit := GetMaxMemoryBytes()
	debug.SetMemoryLimit(newLimit)
	log.Printf("[INFO] Plugin execute, setting memory limit to %dMb", newLimit/(1024*1024))

	outputChan := make(chan *proto.ExecuteResponse, len(req.ExecuteConnectionData))
	errorChan := make(chan error, len(req.ExecuteConnectionData))
	//doneChan := make(chan bool)
	var outputWg sync.WaitGroup

	// get a context which includes telemetry data and logger
	ctx := p.buildExecuteContext(stream.Context(), req, logger)

	// control how many connections are executed in parallel
	maxConcurrentConnections := getMaxConcurrentConnections()
	sem := semaphore.NewWeighted(int64(maxConcurrentConnections))

	// get the config for the connection - needed in case of aggregator
	// NOTE: req.Connection may be empty (for pre v0.19 steampipe versions)
	connectionData := p.ConnectionMap[req.Connection]

	for connectionName := range req.ExecuteConnectionData {
		// if this is an aggregator execution, check whether this child connection supports this table
		if connectionData != nil && connectionData.AggregatedTablesByConnection != nil {
			if tablesForConnection, ok := connectionData.AggregatedTablesByConnection[connectionName]; ok {
				if _, ok := tablesForConnection[req.Table]; !ok {
					log.Printf("[WARN] aggregator connection %s, child connection %s does not provide table %s, skipping",
						connectionData.Connection.Name, connectionName, req.Table)
					continue
				}
			} else {
				// not expected
				log.Printf("[WARN] aggregator connection %s has no data for child connection %s",
					connectionData.Connection.Name, connectionName)
				// just carry on
			}
		}
		outputWg.Add(1)

		go func(c string) {
			defer outputWg.Done()

			if err := sem.Acquire(ctx, 1); err != nil {
				return
			}
			defer sem.Release(1)

			if err := p.executeForConnection(ctx, req, c, outputChan); err != nil {
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
		log.Printf("[TRACE] output wg complete - send nil row (%s)", req.CallId)

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

/*
establishMessageStream establishes a streaming message connection between the plugin and the plugin manager
This is used if the plugin has a dynamic schema and uses file watching

This is the handler function for the establishMessageStream grpc function
*/
func (p *Plugin) establishMessageStream(stream proto.WrapperPlugin_EstablishMessageStreamServer) error {
	log.Printf("[TRACE] plugin.establishMessageStream plugin %p, stream %p", p, stream)
	// if the plugin does not have a dynamic schema, we do not need the message stream
	if p.SchemaMode != SchemaModeDynamic {
		log.Printf("[TRACE] establishMessageStream - plugin %s has static schema so no message stream, required", p.Name)
		return nil
	}

	p.messageStream = stream

	log.Printf("[TRACE] plugin.establishMessageStream set on plugin: plugin.messageStream %p", p.messageStream)

	// hold stream open
	select {}

	return nil
}

func (p *Plugin) setCacheOptions(request *proto.SetCacheOptionsRequest) error {
	return p.ensureCache(p.buildConnectionSchemaMap(), query_cache.NewQueryCacheOptions(request))
}

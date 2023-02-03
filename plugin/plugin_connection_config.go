package plugin

import (
	"context"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/error_helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/context_key"
	"golang.org/x/exp/maps"
	"log"
	"reflect"
	"strings"
)

/*
SetConnectionConfig sets the connection config for the given connection.
(for legacy plugins)
This is the handler function for the SetConnectionConfig GRPC function.
*/
func (p *Plugin) SetConnectionConfig(connectionName, connectionConfigString string) (err error) {
	log.Printf("[TRACE] SetConnectionConfig %s", connectionName)
	failedConnections, err := p.SetAllConnectionConfigs([]*proto.ConnectionConfig{
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
SetAllConnectionConfigs sets the connection config for a list of connections.

This is the handler function for the SetAllConnectionConfigs GRPC function.
*/
func (p *Plugin) SetAllConnectionConfigs(configs []*proto.ConnectionConfig, maxCacheSizeMb int) (failedConnections map[string]error, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("SetAllConnectionConfigs failed: %s", helpers.ToError(r).Error())
		} else {
			p.Logger.Debug("SetAllConnectionConfigs finished")
		}
	}()
	failedConnections = make(map[string]error)

	p.addConnections(configs, failedConnections, nil, nil)

	// TODO report log messages back somewhere
	_, err = p.setAggregatorSchemas()
	if err != nil {
		return failedConnections, err
	}

	// build a connection schema map - used to pass to cache
	connectionSchemaMap := p.buildConnectionSchemaMap()

	// now create the query cache - do this AFTER setting the connection config so we can pass the connection schema map
	err = p.ensureCache(maxCacheSizeMb, connectionSchemaMap)
	if err != nil {
		return failedConnections, err
	}

	// if there are any failed connections, raise an error
	err = error_helpers.CombineErrors(maps.Values(failedConnections)...)
	return failedConnections, err
}

func (p *Plugin) setAggregatorSchemas() (logMessages map[string][]string, err error) {
	// build the schema of all aggregators (do this AFTER adding all connections)
	for _, connectionData := range p.ConnectionMap {
		// if this is an aggregator connection, update its schema, passing its existing config
		if connectionData.isAggregator() {

			logMessages, err := connectionData.initAggregatorSchema(connectionData.config)
			if err != nil {
				return logMessages, nil
			}
		}
	}
	return logMessages, err
}

/*
UpdateConnectionConfigs handles added, changed and deleted connections:

  - Added connections are inserted into [plugin.Plugin.ConnectionMap].

  - Deleted connections are removed from ConnectionMap.

  - For updated connections, ConnectionMap is updated and [plugin.Plugin.ConnectionConfigChangedFunc] is called.

This is the handler function for the UpdateConnectionConfigs GRPC function.
*/
func (p *Plugin) UpdateConnectionConfigs(added []*proto.ConnectionConfig, deleted []*proto.ConnectionConfig, changed []*proto.ConnectionConfig) (failedConnections map[string]error, err error) {
	ctx := context.WithValue(context.Background(), context_key.Logger, p.Logger)

	p.logChanges(added, deleted, changed)

	// if this plugin does not have dynamic config, we can share table map and schema
	var exemplarSchema *grpc.PluginSchema
	var exemplarTableMap map[string]*Table
	if p.SchemaMode == SchemaModeStatic {
		for _, connectionData := range p.ConnectionMap {
			exemplarSchema = connectionData.Schema
			exemplarTableMap = connectionData.TableMap
			// just take the first item
			break
		}
	}

	// key track of connections which have errors
	failedConnections = make(map[string]error)

	// remove deleted connections
	for _, deletedConnection := range deleted {
		delete(p.ConnectionMap, deletedConnection.Connection)
	}

	// add added connections
	p.addConnections(added, failedConnections, exemplarSchema, exemplarTableMap)
	if err != nil {
		return failedConnections, err
	}

	// update changed connections
	// build map of current connection data for each changed connection
	err = p.updateConnections(ctx, changed, failedConnections)
	if err != nil {
		return failedConnections, err
	}

	// if there are any added or changed connections, we need to rebuild all aggregator schemas
	if len(added)+len(deleted)+len(changed) > 0 {
		_, err = p.setAggregatorSchemas()
		if err != nil {
			return failedConnections, err
		}
	}

	// update the query cache schema map
	p.queryCache.PluginSchemaMap = p.buildConnectionSchemaMap()

	return failedConnections, nil
}

func (p *Plugin) updateConnections(ctx context.Context, changed []*proto.ConnectionConfig, failedConnections map[string]error) error {
	if len(changed) == 0 {
		return nil
	}

	// first get the existing connection data - used for the update events
	existingConnections := make(map[string]*ConnectionData, len(changed))
	for _, changedConnection := range changed {
		connectionData, ok := p.ConnectionMap[changedConnection.Connection]
		if !ok {
			return fmt.Errorf("no connection config found for changed connection %s", changedConnection.Connection)
		}
		existingConnections[changedConnection.Connection] = connectionData
	}

	// now just call addConnections for the changed connections
	p.addConnections(changed, failedConnections, nil, nil)

	// call the ConnectionConfigChanged callback function for each changed connection
	for _, changedConnection := range changed {
		c := changedConnection.Connection
		p.ConnectionConfigChangedFunc(ctx, p, existingConnections[c].Connection, p.ConnectionMap[changedConnection.Connection].Connection)
	}
	return nil
}

// add connections for all the provided configs
// NOTE: this (may) mutate failedConnections, exemplarSchema and exemplarTableMap
func (p *Plugin) addConnections(configs []*proto.ConnectionConfig, failedConnections map[string]error, exemplarSchema *grpc.PluginSchema, exemplarTableMap map[string]*Table) {
	log.Printf("[TRACE] SetAllConnectionConfigs setting %d configs", len(configs))

	for _, config := range configs {
		if len(config.ChildConnections) > 0 {
			log.Printf("[TRACE] connection %s is an aggregator - handle separately", config.Connection)
			p.setAggregatorConnectionData(config)
		} else {
			p.setConnectionData(config, failedConnections, exemplarSchema, exemplarTableMap)
		}
	}
}

// create a connectionData for this connection and store it on the plugin
// ConnectionData contains the table map, the schema and the connection
// NOTE: this (may) mutate failedConnections, exemplarSchema and exemplarTableMap
// NOTE: this is NOT called for aggregator connections
func (p *Plugin) setConnectionData(config *proto.ConnectionConfig, failedConnections map[string]error, exemplarSchema *grpc.PluginSchema, exemplarTableMap map[string]*Table) {
	connectionName := config.Connection
	connectionConfigString := config.Config
	if connectionName == "" {
		log.Printf("[WARN] setConnectionData failed - ConnectionConfig contained empty connection name")
		failedConnections["unknown"] = fmt.Errorf("setConnectionData failed - ConnectionConfig contained empty connection name")
		return
	}

	// create connection object
	c := &Connection{Name: connectionName}
	// if config was provided, parse it
	if connectionConfigString != "" {
		if p.ConnectionConfigSchema == nil {
			failedConnections[connectionName] = fmt.Errorf("connection config has been set for connection '%s', but plugin '%s' does not define connection config schema", connectionName, p.Name)
			return
		}
		// ask plugin for a struct to deserialise the config into
		config, err := p.ConnectionConfigSchema.parse(config)
		if err != nil {
			failedConnections[connectionName] = err
			log.Printf("[WARN] setConnectionData failed for connection %s, config validation failed: %s", connectionName, err.Error())
			return
		}
		c.Config = config
	}

	var err error
	// set table map ands schema exemplar
	schema := exemplarSchema
	tableMap := exemplarTableMap

	// if tableMap is nil, exemplar is not yet set
	if tableMap == nil {
		log.Printf("[TRACE] connection %s build schema and table map", connectionName)
		tableMap, schema, err = p.getConnectionSchema(c)
		if err != nil {
			failedConnections[connectionName] = err
			return
		}

		if p.SchemaMode == SchemaModeStatic {
			exemplarSchema = schema
			exemplarTableMap = tableMap
		}
	}

	// add to connection map
	d := NewConnectionData(c, p, config).setSchema(tableMap, schema)
	p.ConnectionMap[connectionName] = d

	log.Printf("[TRACE] SetAllConnectionConfigs added connection %s to map, setting watch paths", c.Name)

	// update the watch paths for the connection file watcher
	err = p.updateConnectionWatchPaths(c)
	if err != nil {
		log.Printf("[WARN] SetAllConnectionConfigs failed to update the watched paths for connection %s: %s", c.Name, err.Error())
		failedConnections[connectionName] = err
	}
	return
}

func (p *Plugin) getConnectionSchema(c *Connection) (map[string]*Table, *grpc.PluginSchema, error) {
	ctx := context.WithValue(context.Background(), context_key.Logger, p.Logger)

	// if the plugin defines a CreateTables func, call it now
	tableMap, err := p.initialiseTables(ctx, c)
	if err != nil {
		return nil, nil, err
	}

	// populate the plugin schema
	schema, err := p.buildSchema(tableMap)
	if err != nil {
		return nil, nil, err
	}
	return tableMap, schema, err
}

func (p *Plugin) updateConnectionWatchPaths(c *Connection) error {
	if watchPaths := p.extractWatchPaths(c.Config); len(watchPaths) > 0 {
		log.Printf("[TRACE] updateConnectionWatchPaths for connection %s, watch paths: %v", c.Name, watchPaths)
		connectionData := p.ConnectionMap[c.Name]
		err := connectionData.updateWatchPaths(watchPaths, p)
		if err != nil {
			return err
		}
	}
	return nil
}

// reflect on a config struct and extract any watch paths, using the `watch` tag
func (p *Plugin) extractWatchPaths(config interface{}) []string {
	if helpers.IsNil(config) {
		return nil
	}

	val := reflect.ValueOf(config)
	valType := val.Type()
	var watchedProperties []string
	for i := 0; i < val.Type().NumField(); i++ {
		// does this property have a steampipe tag
		field := valType.Field(i)
		steampipeTag := field.Tag.Get("steampipe")
		if steampipeTag != "" {
			steampipeTagLabels := strings.Split(steampipeTag, ",")
			// does the tag have a 'watch' label?
			if helpers.StringSliceContains(steampipeTagLabels, "watch") {
				// get property value
				if value, ok := helpers.GetFieldValueFromInterface(config, valType.Field(i).Name); ok {
					if arrayVal, ok := value.([]string); ok {
						for _, val := range arrayVal {
							watchedProperties = append(watchedProperties, val)
						}
					} else if stringVal, ok := value.(string); ok {
						watchedProperties = append(watchedProperties, stringVal)
					}
				}
			}
		}
	}
	return watchedProperties
}

func (p *Plugin) logChanges(added []*proto.ConnectionConfig, deleted []*proto.ConnectionConfig, changed []*proto.ConnectionConfig) {
	// build list of names
	addedNames := make([]string, len(added))
	deletedNames := make([]string, len(deleted))
	changedNames := make([]string, len(changed))
	for i, c := range added {
		addedNames[i] = c.Connection
	}
	for i, c := range deleted {
		deletedNames[i] = c.Connection
	}
	for i, c := range changed {
		changedNames[i] = c.Connection
	}
	log.Printf("[TRACE] UpdateConnectionConfigs added: %s, deleted: %s, changed: %s", strings.Join(addedNames, ","), strings.Join(deletedNames, ","), strings.Join(changedNames, ","))
}

// this is the default ConnectionConfigChanged callback function
// it clears both the query cache and connection cache for the given connection
func defaultConnectionConfigChangedFunc(ctx context.Context, p *Plugin, old *Connection, new *Connection) error {
	log.Printf("[TRACE] defaultConnectionConfigChangedFunc connection config changed for connection: %s", new.Name)
	p.ClearConnectionCache(ctx, new.Name)
	p.ClearQueryCache(ctx, new.Name)
	return nil
}

// this is the default WatchedFilesChangedFunc callback function
// it clears both the query cache and connection cache for the given connection
func defaultWatchedFilesChangedFunc(ctx context.Context, p *Plugin, conn *Connection, events []fsnotify.Event) {
	log.Printf("[TRACE] defaultWatchedFilesChangedFunc filewatchers changed for connection %s", conn.Name)
	p.ClearConnectionCache(ctx, conn.Name)
	p.ClearQueryCache(ctx, conn.Name)
}

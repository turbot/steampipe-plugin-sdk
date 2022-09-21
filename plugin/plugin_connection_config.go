package plugin

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/context_key"
)

/*
SetConnectionConfig sets the connection config for the given connection.

This is the handler function for the SetConnectionConfig GRPC function.
*/
func (p *Plugin) SetConnectionConfig(connectionName, connectionConfigString string) (err error) {
	log.Printf("[TRACE] SetConnectionConfig %s", connectionName)
	return p.SetAllConnectionConfigs([]*proto.ConnectionConfig{
		{
			Connection: connectionName,
			Config:     connectionConfigString,
		},
	}, 0)
}

/*
SetAllConnectionConfigs sets the connection config for a list of connections.

This is the handler function for the SetAllConnectionConfigs GRPC function.
*/
func (p *Plugin) SetAllConnectionConfigs(configs []*proto.ConnectionConfig, maxCacheSizeMb int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("SetAllConnectionConfigs failed: %s", helpers.ToError(r).Error())
		} else {
			p.Logger.Debug("SetAllConnectionConfigs finished")
		}
	}()

	log.Printf("[TRACE] SetAllConnectionConfigs setting %d configs", len(configs))

	// if this plugin does not have dynamic config, we can share table map and schema
	var exemplarSchema map[string]*proto.TableSchema
	var exemplarTableMap map[string]*Table

	var aggregators []*proto.ConnectionConfig
	for _, config := range configs {
		// NOTE: do not set connection config for aggregator connections
		if len(config.ChildConnections) > 0 {
			log.Printf("[TRACE] connection %s is an aggregator - handle separately", config.Connection)
			aggregators = append(aggregators, config)
			continue
		}

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
			config, err := p.ConnectionConfigSchema.parse(connectionConfigString)
			if err != nil {
				return err
			}

			c.Config = config
		}

		schema := exemplarSchema
		tableMap := exemplarTableMap
		var err error

		if tableMap == nil {
			log.Printf("[TRACE] connection %s build schema and table map", connectionName)
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
		log.Printf("[WARN] SetAllConnectionConfigs added connection %s to map, setting watch paths", c.Name)

		// set watch paths
		err = p.updateConnectionWatchPaths(c)
		if err != nil {
			return err
		}
	}

	for _, aggregatorConfig := range aggregators {
		firstChild := p.ConnectionMap[aggregatorConfig.ChildConnections[0]]
		// we do not currently support aggregator connections for dynamic schema
		if p.SchemaMode == SchemaModeDynamic {
			return fmt.Errorf("aggregator connections are not supported for dynamic plugins: connection '%s', plugin: '%s'", aggregatorConfig.Connection, aggregatorConfig.Plugin)
		}

		// add to connection map using the first child's schema
		p.ConnectionMap[aggregatorConfig.Connection] = &ConnectionData{
			TableMap:   firstChild.TableMap,
			Connection: &Connection{Name: aggregatorConfig.Connection},
			Schema:     firstChild.Schema,
		}
	}

	// now create the query cache - do this AFTER setting the connection config so the cache can build
	// the connection schema map
	p.ensureCache(maxCacheSizeMb)

	return nil
}

/*
UpdateConnectionConfigs handles added, changed and deleted connections:

  - Added connections are inserted into [plugin.Plugin.ConnectionMap].

  - Deleted connections are removed from ConnectionMap.

  - For updated connections, ConnectionMap is updated and [plugin.Plugin.ConnectionConfigChangedFunc] is called.

This is the handler function for the UpdateConnectionConfigs GRPC function.
*/
func (p *Plugin) UpdateConnectionConfigs(added []*proto.ConnectionConfig, deleted []*proto.ConnectionConfig, changed []*proto.ConnectionConfig) error {
	p.logChanges(added, deleted, changed)

	// if this plugin does not have dynamic config, we can share table map and schema
	var exemplarSchema map[string]*proto.TableSchema
	var exemplarTableMap map[string]*Table
	if p.SchemaMode == SchemaModeStatic {
		for _, connectionData := range p.ConnectionMap {
			exemplarSchema = connectionData.Schema
			exemplarTableMap = connectionData.TableMap
		}
	}

	// remove deleted connections
	for _, deletedConnection := range deleted {
		delete(p.ConnectionMap, deletedConnection.Connection)
	}

	// add added connections
	for _, addedConnection := range added {
		schema := exemplarSchema
		tableMap := exemplarTableMap
		// create connection object
		c := &Connection{
			Name:   addedConnection.Connection,
			Config: addedConnection.Config,
		}
		if addedConnection.Config != "" {
			if p.ConnectionConfigSchema == nil {
				return fmt.Errorf("connection config has been set for connection '%s', but plugin '%s' does not define connection config schema", addedConnection.Connection, p.Name)
			}
			// ask plugin to parse the config
			config, err := p.ConnectionConfigSchema.parse(addedConnection.Config)
			if err != nil {
				return err
			}

			c.Config = config
		}

		if p.SchemaMode == SchemaModeDynamic {
			var err error
			log.Printf("[TRACE] UpdateConnectionConfigs - connection %s build schema and table map", addedConnection.Connection)
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
		}

		log.Printf("[WARN] UpdateConnectionConfigs added connection %s to map, setting watch paths", c.Name)

		err := p.updateConnectionWatchPaths(c)
		if err != nil {
			return err
		}

		p.ConnectionMap[addedConnection.Connection] = &ConnectionData{
			TableMap:   tableMap,
			Connection: c,
			Schema:     schema,
		}
		// after adding the connection data, update the watch paths
		p.updateConnectionWatchPaths(c)
	}

	// update the query cache schema map
	connectionSchemaMap := p.buildConnectionSchemaMap()
	p.queryCache.PluginSchemaMap = connectionSchemaMap

	ctx := context.WithValue(context.Background(), context_key.Logger, p.Logger)

	for _, changedConnection := range changed {
		// get the existing connection data
		connectionData, ok := p.ConnectionMap[changedConnection.Connection]
		if !ok {
			return fmt.Errorf("no connection config found for changed connection %s", changedConnection.Connection)
		}
		existingConnection := connectionData.Connection
		updatedConnection := &Connection{
			Name:   changedConnection.Connection,
			Config: changedConnection.Config,
		}
		if p.ConnectionConfigSchema == nil {
			return fmt.Errorf("connection config has been updated for connection '%s', but plugin '%s' does not define connection config schema", changedConnection.Connection, p.Name)
		}
		// ask plugin to parse the config
		config, err := p.ConnectionConfigSchema.parse(changedConnection.Config)
		if err != nil {
			return err
		}
		updatedConnection.Config = config

		// now update connectionData and write back
		connectionData.Connection = updatedConnection
		p.ConnectionMap[changedConnection.Connection] = connectionData

		log.Printf("[WARN] UpdateConnectionConfigs update connection %s in map, setting watch paths", changedConnection.Connection)

		err = p.updateConnectionWatchPaths(updatedConnection)
		if err != nil {
			return err
		}

		// call the ConnectionConfigChanged callback function
		p.ConnectionConfigChangedFunc(ctx, p, existingConnection, updatedConnection)

	}

	return nil
}

func (p *Plugin) updateConnectionWatchPaths(c *Connection) error {
	if watchPaths := p.extractWatchPaths(c.Config); len(watchPaths) > 0 {
		log.Printf("[WARN] updateConnectionWatchPaths for connection %s, watch paths: %v", c.Name, watchPaths)
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
						watchedProperties = append(watchedProperties, arrayVal...)
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

// this is the default ConnectionConfigChanged callback function - it clears both the query cache and connection cache
// for the given connection
func defaultConnectionConfigChangedFunc(ctx context.Context, p *Plugin, old *Connection, new *Connection) error {
	// clear the connection and query cache for this connection
	p.ClearConnectionCache(ctx, new.Name)
	p.ClearQueryCache(ctx, new.Name)
	return nil
}

// this is the default WatchedFilesChangedFunc callback function - it clears both the query cache and
// connection cache for the given connection
func defaultWatchedFilesChangedFunc(ctx context.Context, p *Plugin, conn *Connection, events []fsnotify.Event) {
	// clear the connection and query cache for this connection
	p.ClearConnectionCache(ctx, conn.Name)
	p.ClearQueryCache(ctx, conn.Name)
}

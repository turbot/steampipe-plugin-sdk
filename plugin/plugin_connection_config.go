package plugin

import (
	"context"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/context_key"
	"log"
	"reflect"
	"strings"
)

func (p *Plugin) setAggregatorSchemas() (logMessages map[string][]string, err error) {
	// build the schema of all aggregators (do this AFTER adding all connections)
	for _, connectionData := range p.ConnectionMap {
		// if this is an aggregator connection, update its schema, passing its existing config
		if connectionData.isAggregator() {
			logMessages, err := connectionData.initAggregatorSchema(connectionData.config)
			if err != nil {
				return logMessages, err
			}
		}
	}
	return logMessages, nil
}

func (p *Plugin) updateConnections(ctx context.Context, changed []*proto.ConnectionConfig, updateData *connectionUpdateData) {
	if len(changed) == 0 {
		return
	}
	log.Printf("[INFO] updateConnections updating %d connections", len(changed))

	if len(changed) == 0 {
		return
	}

	// first get the existing connection data - used for the update events
	existingConnections := make(map[string]*ConnectionData, len(changed))
	for _, changedConnection := range changed {
		connectionData, ok := p.ConnectionMap[changedConnection.Connection]
		if !ok {
			updateData.failedConnections[changedConnection.Connection] = fmt.Errorf("no connection config found for changed connection %s", changedConnection.Connection)
			return
		}
		existingConnections[changedConnection.Connection] = connectionData
	}

	// now just call addConnections for the changed connections
	p.addConnections(changed, updateData)

	// call the ConnectionConfigChanged callback function for each changed connection
	for _, changedConnection := range changed {
		c := changedConnection.Connection
		p.ConnectionConfigChangedFunc(ctx, p, existingConnections[c].Connection, p.ConnectionMap[changedConnection.Connection].Connection)
	}
	return
}

// add connections for all the provided configs
// NOTE: this (may) mutate failedConnections, exemplarSchema and exemplarTableMap
func (p *Plugin) addConnections(configs []*proto.ConnectionConfig, updateData *connectionUpdateData) {
	if len(configs) == 0 {
		return
	}

	log.Printf("[INFO] addConnections adding %d connection configs", len(configs))

	for _, config := range configs {
		if config.IsAggregator() {
			log.Printf("[TRACE] connection %s is an aggregator - handle separately", config.Connection)
			p.setAggregatorConnectionData(config)
		} else {
			p.setConnectionData(config, updateData)
		}
	}
}

// create a connectionData for this connection and store it on the plugin
// ConnectionData contains the table map, the schema and the connection
// NOTE: this (may) mutate failedConnections, exemplarSchema and exemplarTableMap
// NOTE: this is NOT called for aggregator connections
func (p *Plugin) setConnectionData(config *proto.ConnectionConfig, updateData *connectionUpdateData) {
	connectionName := config.Connection
	connectionConfigString := config.Config
	if connectionName == "" {
		log.Printf("[WARN] setConnectionData failed - ConnectionConfig contained empty connection name")
		updateData.failedConnections["unknown"] = fmt.Errorf("setConnectionData failed - ConnectionConfig contained empty connection name")
		return
	}

	// create connection object
	c := &Connection{Name: connectionName}
	// if config was provided, parse it
	if connectionConfigString != "" {
		if p.ConnectionConfigSchema == nil {
			updateData.failedConnections[connectionName] = fmt.Errorf("connection config has been set for connection '%s', but plugin '%s' does not define connection config schema", connectionName, p.Name)
			return
		}
		// ask plugin for a struct to deserialise the config into
		config, err := p.ConnectionConfigSchema.parse(config)
		if err != nil {
			updateData.failedConnections[connectionName] = err
			log.Printf("[WARN] setConnectionData failed for connection %s, config validation failed: %s", connectionName, err.Error())
			return
		}
		c.Config = config
	}

	var err error
	// set table map ands schema exemplar
	schema := updateData.exemplarSchema
	tableMap := updateData.exemplarTableMap

	// if tableMap is nil, exemplar is not yet set
	if tableMap == nil {
		log.Printf("[TRACE] connection %s build schema and table map", connectionName)
		tableMap, schema, err = p.getConnectionSchema(c)
		if err != nil {
			updateData.failedConnections[connectionName] = err
			return
		}

		if p.SchemaMode == SchemaModeStatic {
			updateData.exemplarSchema = schema
			updateData.exemplarTableMap = tableMap
		}
	}

	// add to connection map
	d := NewConnectionData(c, p, config).setSchema(tableMap, schema)
	p.ConnectionMap[connectionName] = d

	log.Printf("[INFO] SetAllConnectionConfigs added connection %s to map, setting watch paths", c.Name)

	// update the watch paths for the connection file watcher
	err = p.updateConnectionWatchPaths(c)
	if err != nil {
		log.Printf("[WARN] SetAllConnectionConfigs failed to update the watched paths for connection %s: %s", c.Name, err.Error())
		updateData.failedConnections[connectionName] = err
	}
	return
}

func (p *Plugin) getConnectionSchema(c *Connection) (map[string]*Table, *grpc.PluginSchema, error) {
	ctx := context.WithValue(context.Background(), context_key.Logger, p.Logger)

	// initialiseTables - if the plugin defines a TableMapFunc func, call it now
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
	log.Printf("[INFO] UpdateConnectionConfigs added: %s, deleted: %s, changed: %s", strings.Join(addedNames, ","), strings.Join(deletedNames, ","), strings.Join(changedNames, ","))
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

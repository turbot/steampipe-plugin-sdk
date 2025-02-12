package plugin

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"slices"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/gertd/go-pluralize"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/context_key"
	"github.com/turbot/steampipe-plugin-sdk/v5/sperr"
)

func (p *Plugin) setAggregatorSchemas() (logMessages map[string][]string, err error) {
	// build the schema of all aggregators (do this AFTER adding all connections)
	p.connectionMapLock.RLock()
	defer p.connectionMapLock.RUnlock()

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

	// first get the existing connection - used for the update events
	existingConnections := make(map[string]*Connection, len(changed))
	for _, changedConnection := range changed {
		connectionData, ok := p.getConnectionData(changedConnection.Connection)
		if !ok {
			// not expected
			updateData.failedConnections[changedConnection.Connection] = fmt.Errorf("no connection config found for changed connection %s", changedConnection.Connection)
			return
		}
		// clone the existing connection and store (it may get updated by upsertConnections)
		existingConnections[changedConnection.Connection] = connectionData.Connection.shallowCopy()
	}

	// now just call upsertConnections for the changed connections -
	p.upsertConnections(changed, updateData)

	// call the ConnectionConfigChanged callback function for each changed connection
	for _, changedConnection := range changed {
		c := changedConnection.Connection
		connectionData, ok := p.getConnectionData(changedConnection.Connection)
		if !ok {
			// possible this connection may have failed to parse or something
			// in which case there will be an error in updateData
			continue
		}
		p.ConnectionConfigChangedFunc(ctx, p, existingConnections[c], connectionData.Connection)
	}
	return
}

func (p *Plugin) getExemplarConnectionData() *ConnectionData {
	p.connectionMapLock.RLock()
	defer p.connectionMapLock.RUnlock()

	for _, connectionData := range p.ConnectionMap {
		// just take the first item
		return connectionData
	}

	return nil
}

func (p *Plugin) deleteConnections(deleted []*proto.ConnectionConfig) {
	if len(deleted) > 0 {
		deletedNames := make([]string, len(deleted))
		for i, c := range deleted {
			deletedNames[i] = c.Connection
		}
		p.deleteConnectionData(deletedNames)
	}
}

// add/update connections for all the provided configs
// NOTE: this (may) mutate failedConnections, exemplarSchema and exemplarTableMap
func (p *Plugin) upsertConnections(configs []*proto.ConnectionConfig, updateData *connectionUpdateData) {
	if len(configs) == 0 {
		return
	}

	log.Printf("[INFO] upsertConnections adding %d connection %s",
		len(configs),
		pluralize.NewClient().Pluralize("connection", len(configs), false))

	for _, config := range configs {
		if config.IsAggregator() {
			log.Printf("[TRACE] connection %s is an aggregator - handle separately", config.Connection)
			p.createAggregatorConnectionData(config)
		} else {
			p.upsertConnectionData(config, updateData)
		}
	}
}

// create or update connectionData for this connection and store it on the plugin
// ConnectionData contains the table map, the schema and the connection
// NOTE: this (may) mutate failedConnections, exemplarSchema and exemplarTableMap
// NOTE: this is NOT called for aggregator connections
func (p *Plugin) upsertConnectionData(config *proto.ConnectionConfig, updateData *connectionUpdateData) {
	connectionName := config.Connection

	if connectionName == "" {
		log.Printf("[WARN] upsertConnectionData failed - ConnectionConfig contained empty connection name")
		updateData.failedConnections["unknown"] = fmt.Errorf("upsertConnectionData failed - ConnectionConfig contained empty connection name")
		return
	}

	// if config was provided, parse it
	// (this returns nil if there was no config - that is ok
	configStruct, err := p.parseConnectionConfig(config)
	if err != nil {
		updateData.failedConnections[connectionName] = err
		return
	}

	// if there is already connection data in the map for this connection, update it
	// (and specifically - update the Connection object instead of replacing it)
	// this is because its possible a query is executing already with the Connection object in it's QueryData
	// if we replace the Connection with a new struct, any update we make to the Connection.Config will not be
	// picked up by those running queries
	// worst case scenario is that (for example) the Aws plugin may refresh the Client using the previous credentials
	d, alreadyHaveConnectionData := p.getConnectionData(connectionName)
	if !alreadyHaveConnectionData {
		// no data stored for this connection - create
		c := &Connection{Name: connectionName}
		d = NewConnectionData(c, p, config)
	}

	// set config struct (may be nil)
	d.Connection.Config = configStruct

	// set the schema

	// set table map ands schema exemplar
	schema, tableMap, err := p.getExemplarSchemaFromUpdateData(updateData, d.Connection)
	if err != nil {
		updateData.failedConnections[connectionName] = err
		return
	}
	d.setSchema(tableMap, schema)

	// add to connection map if needed
	if !alreadyHaveConnectionData {
		p.setConnectionData(d, connectionName)
	}

	log.Printf("[INFO] SetAllConnectionConfigs added connection %s to map, setting watch paths", connectionName)

	// update the watch paths for the connection file watcher
	err = p.updateConnectionWatchPaths(d.Connection)
	if err != nil {
		log.Printf("[WARN] SetAllConnectionConfigs failed to update the watched paths for connection %s: %s", connectionName, err.Error())
		updateData.failedConnections[connectionName] = err
	}
	return
}

func (p *Plugin) getExemplarSchemaFromUpdateData(updateData *connectionUpdateData, c *Connection) (*grpc.PluginSchema, map[string]*Table, error) {
	schema := updateData.exemplarSchema
	tableMap := updateData.exemplarTableMap
	var err error
	// if tableMap is nil, exemplar is not yet set
	if tableMap == nil {
		log.Printf("[TRACE] connection %s build schema and table map", c.Name)
		tableMap, schema, err = p.getConnectionSchema(c)
		if err != nil {
			return nil, nil, err
		}

		if p.SchemaMode == SchemaModeStatic {
			updateData.exemplarSchema = schema
			updateData.exemplarTableMap = tableMap
		}
	}
	return schema, tableMap, nil
}

func (p *Plugin) parseConnectionConfig(config *proto.ConnectionConfig) (any, error) {
	if config.Config == "" {
		return nil, nil
	}

	if p.ConnectionConfigSchema == nil {
		msg := fmt.Sprintf("connection config has been set for connection '%s', but plugin '%s' does not define connection config schema", config.Connection, p.Name)
		log.Println("[WARN]", msg)
		return nil, sperr.New(msg)

	}
	// parse the config into a struct
	configStruct, err := p.ConnectionConfigSchema.parse(config)
	if err != nil {
		log.Printf("[WARN] upsertConnectionData failed for connection %s, config validation failed: %s", config.Connection, err.Error())
		return nil, err
	}

	return configStruct, err
}

func (p *Plugin) getConnectionSchema(c *Connection) (map[string]*Table, *grpc.PluginSchema, error) {
	log.Printf("[INFO] getConnectionSchema for connection %s", c.Name)
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
	watchPaths := p.extractWatchPaths(c.Config)
	if len(watchPaths) == 0 {
		return nil
	}
	log.Printf("[TRACE] updateConnectionWatchPaths for connection %s, watch paths: %v", c.Name, watchPaths)
	connectionData, ok := p.getConnectionData(c.Name)
	if !ok {
		return nil
	}
	return connectionData.updateWatchPaths(watchPaths, p)
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
			if slices.Contains(steampipeTagLabels, "watch") {
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
	if err := p.ClearConnectionCache(ctx, new.Name); err != nil {
		return err
	}
	if err := p.ClearQueryCache(ctx, new.Name); err != nil {
		return err
	}

	return nil
}

// this is the default WatchedFilesChangedFunc callback function
// it clears both the query cache and connection cache for the given connection
func defaultWatchedFilesChangedFunc(ctx context.Context, p *Plugin, conn *Connection, events []fsnotify.Event) {
	log.Printf("[TRACE] defaultWatchedFilesChangedFunc filewatchers changed for connection %s", conn.Name)
	if err := p.ClearConnectionCache(ctx, conn.Name); err != nil {
		log.Printf("[WARN] failed to clear connection cache for connection %s, error: %s", conn.Name, err.Error())
	}
	if err := p.ClearQueryCache(ctx, conn.Name); err != nil {
		log.Printf("[WARN] failed to clear query cache for connection %s, error: %s", conn.Name, err.Error())
	}
}

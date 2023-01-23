package plugin

import (
	"context"
	"fmt"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
	"path"

	"github.com/fsnotify/fsnotify"
	"github.com/turbot/go-kit/filewatcher"
	"github.com/turbot/steampipe-plugin-sdk/v5/getter"
)

// ConnectionData is the data stored by the plugin which is connection dependent.
type ConnectionData struct {
	// map of all the tables in the plugin, keyed by the table name
	TableMap   map[string]*Table
	Connection *Connection
	// schema may be connection specific for dynamic schemas
	Schema *grpc.PluginSchema
	// this is connection specific filewatcher to watch for the changes in files (if needed)
	Watcher *filewatcher.FileWatcher
	Plugin  *Plugin

	// map of aggregated tables, keyed by connection name
	AggregatedTablesByConnection map[string]map[string]*Table
}

func NewConnectionData(c *Connection, tableMap map[string]*Table, schema *grpc.PluginSchema, p *Plugin) *ConnectionData {
	return &ConnectionData{
		TableMap:   tableMap,
		Connection: c,
		Schema:     schema,
		Plugin:     p,
	}
}

// GetConnectionTempDir appends the connection name to the plugin temporary directory path
func (d *ConnectionData) GetConnectionTempDir(pluginTempDir string) string {
	return path.Join(pluginTempDir, d.Connection.Name)
}

func (d *ConnectionData) updateWatchPaths(watchPaths []string, p *Plugin) error {
	// close any existing watcher
	if d.Watcher != nil {
		log.Printf("[TRACE] ConnectionData updateWatchPaths - close existing watcher")
		d.Watcher.Close()
		d.Watcher = nil
	}

	// create WatcherOptions
	connTempDir := d.GetConnectionTempDir(p.tempDir)
	opts := filewatcher.WatcherOptions{
		EventMask: fsnotify.Create | fsnotify.Write | fsnotify.Remove | fsnotify.Rename,
	}

	// Iterate through watch paths to resolve and
	// add resolved paths to file watcher options
	log.Printf("[TRACE] ConnectionData.updateWatchPaths - create watcher options from the watchPaths %v", watchPaths)
	for _, watchPath := range watchPaths {
		dest, globPattern, err := getter.GetFiles(watchPath, connTempDir)
		if err != nil {
			log.Printf("[WARN] ConnectionData updateWatchPaths - error resolving source path %s: %s", watchPath, err.Error())
			continue
		}
		opts.Directories = append(opts.Directories, dest)
		opts.Include = append(opts.Include, globPattern)
	}

	// if we have no paths, do not start a watcher
	if len(opts.Directories) == 0 {
		log.Printf("[TRACE] ConnectionData updateWatchPaths - no watch paths resolved - not creating watcher")
		return nil
	}
	// Add the callback function for the filewatchers to watcher options
	opts.OnChange = func(events []fsnotify.Event) {
		log.Printf("[TRACE] watched connection files changed")
		// call the callback
		p.WatchedFileChangedFunc(context.Background(), p, d.Connection, events)
		// if this plugin has a dynamic schema, rebuild the schema and notify if it has changed
		if p.SchemaMode == SchemaModeDynamic {
			log.Printf("[TRACE] watched connection files updated schema")
			if err := p.ConnectionSchemaChanged(d.Connection); err != nil {
				log.Printf("[WARN] failed to update plugin schema after file event: %s", err.Error())
			}
		}
	}

	// Get the new file watcher from file options
	newWatcher, err := filewatcher.NewWatcher(&opts)
	if err != nil {
		log.Printf("[WARN] ConnectionData.updateWatchPaths - failed to create a new file watcher: %s", err.Error())
		return err
	}
	log.Printf("[TRACE] ConnectionData.updateWatchPaths - created the new file watcher")

	// Start new watcher
	newWatcher.Start()

	// Assign new watcher to the connection
	d.Watcher = newWatcher
	return nil
}

// if this connection data is for a dynamic aggregator, build a map of the tables which each child connection provides
func (d *ConnectionData) initAggregatorSchema(aggregatorConfig *proto.ConnectionConfig) (map[string][]string, error) {
	// TODO do we need to cache this - is it the right thing to cache????
	d.AggregatedTablesByConnection = make(map[string]map[string]*Table)
	logMessages := make(map[string][]string)
	for _, c := range aggregatorConfig.ChildConnections {
		// find connection data for this child connection
		connectionData, ok := d.Plugin.ConnectionMap[c]
		if !ok {
			// should never happen
			return nil, fmt.Errorf("initAggregatorSchema failed for aggregator connection '%s': no connection data loaded for child connection '%s'", aggregatorConfig.Connection, c)
		}

		// ask the connection data to build a list of tables that this connection provides
		// this takes into account:
		// - table level `Aggregation` property
		// - aggregator connection config TableAggregationSpecs
		d.AggregatedTablesByConnection[c] = connectionData.getAggregatedTables(aggregatorConfig)
	}
	// resolve tables to include
	for connectionName, tableMap := range d.AggregatedTablesByConnection {
		for tableName, table := range tableMap {
			// if this is already in the map, that implies it has already been validated
			// (i.e. its schema IS the same for all (included) connections)
			// skip
			if _, ok := d.TableMap[tableName]; ok {
				continue
			}

			if schema, isSame := d.tableSchemaSameForAllConnections(tableName); isSame {
				d.TableMap[tableName] = table
				d.Schema.Schema[tableName] = schema
			} else {
				// TODO use standard codes/enum?
				logMessages[connectionName] = append(logMessages[connectionName], fmt.Sprintf("Excluding table %s.%s as the schema is not the same for all connections", connectionName, tableName))
				// NOTE: remove this table from ALL entries in AggregatedTablesByConnection
				for _, tableMap := range d.AggregatedTablesByConnection {
					delete(tableMap, tableName)
				}
			}
		}
	}
	return logMessages, nil
}

func (d *ConnectionData) includeTableInAggregator(connectionName string, table *Table, aggregatorConfig *proto.ConnectionConfig) bool {
	// see if there is a matching table spec
	if aggregationSpec := aggregatorConfig.GetAggregationSpecForTable(table.Name); aggregationSpec != nil {
		// if a table is matched by the connection config, this overrides any table defintion `Aggregation` props
		// TODO what if more than one spec matches table - currently first one wins
		return aggregationSpec.MatchesConnection(connectionName)
	}

	// otherwise if the config does not specify this table, fall back onto the table a`Aggregation` property
	return table.Aggregation != AggregationModeNone
}

// build a map of tables to include in an aggregator using this connection
// take into account tables excluded by the table def AND by the connection config
func (d *ConnectionData) getAggregatedTables(aggregatorConfig *proto.ConnectionConfig) map[string]*Table {
	res := make(map[string]*Table)
	for name, table := range d.TableMap {
		if d.includeTableInAggregator(d.Connection.Name, table, aggregatorConfig) {
			res[name] = table
		}
	}
	return res
}

func (d *ConnectionData) tableSchemaSameForAllConnections(tableName string) (*proto.TableSchema, bool) {
	p := d.Plugin
	var exemplarSchema *proto.TableSchema
	for _, c := range p.ConnectionMap {
		// does this connection have this table?
		tableSchema, ok := c.Schema.Schema[tableName]
		if !ok {
			log.Printf("[TRACE] tableSchemaSameForAllConnections: connection %s does not provide table %s", c.Connection.Name, tableName)
			continue
		}
		if exemplarSchema == nil {
			exemplarSchema = tableSchema
		} else if !tableSchema.Equals(exemplarSchema) {
			return nil, false
		}
	}
	return exemplarSchema, true

}

package plugin

import (
	"context"
	"fmt"
	"github.com/gertd/go-pluralize"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"golang.org/x/exp/maps"
	"log"
	"path"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/turbot/go-kit/filewatcher"
	"github.com/turbot/steampipe-plugin-sdk/v5/getter"
)

// ConnectionData is the data stored by the plugin which is connection dependent.
type ConnectionData struct {
	// map of all the tables in the connection schema, keyed by the table name
	TableMap   map[string]*Table
	Connection *Connection
	// the connection schema
	Schema *grpc.PluginSchema
	// connection specific filewatcher to watch for the changes in files (if needed)
	Watcher *filewatcher.FileWatcher
	Plugin  *Plugin

	// raw connection config
	config *proto.ConnectionConfig

	// map of aggregated tables, keyed by connection name
	// NOTE: only set for aggregator connections
	AggregatedTablesByConnection map[string]map[string]*Table
}

func NewConnectionData(c *Connection, p *Plugin, config *proto.ConnectionConfig) *ConnectionData {
	return &ConnectionData{
		Connection: c,
		Plugin:     p,
		config:     config,
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
func (d *ConnectionData) initAggregatorSchema(aggregatorConfig *proto.ConnectionConfig) (logMessages map[string][]string, err error) {
	// build map of the tables provided by each child connection
	// this takes into account the table 'Aggregation' property and the aggregator config TableAggregationSpecs
	logMessages, err = d.getAggregatedTablesByConnection(aggregatorConfig)
	if err != nil {
		return logMessages, err
	}

	// resolve tables to include (only include tables who have same schema for all connections)
	d.resolveAggregatorTableMap(logMessages)
	// log out the schema init process
	d.logInitAggregatorSchema(aggregatorConfig)

	return logMessages, nil
}

func (d *ConnectionData) logInitAggregatorSchema(aggregatorConfig *proto.ConnectionConfig) {
	log.Printf("[INFO] -------------------------------")
	log.Printf("[INFO] Initialising aggregator schema ")
	log.Printf("[INFO] -------------------------------")
	log.Printf("[INFO] Aggregator: '%s'", aggregatorConfig.Connection)
	log.Printf("[INFO] ")
	log.Printf("[INFO] ")
	log.Printf("[INFO] Tables provided by child connections:")
	for c, tables := range d.AggregatedTablesByConnection {
		log.Printf("[INFO] \t%s: %s", c, strings.Join(maps.Keys(tables), ","))
	}
	log.Printf("[INFO] ")
	log.Printf("[INFO] ")
	log.Printf("[INFO] Schema tables: %s", strings.Join(maps.Keys(d.TableMap), ","))
	log.Printf("[INFO] ")
}

// for each table in AggregatedTablesByConnection, verify all connections havde the same schema and if so,
// add to table map
func (d *ConnectionData) resolveAggregatorTableMap(logMessages map[string][]string) {
	// clear table map and schema before we start
	d.TableMap = make(map[string]*Table)
	d.Schema = grpc.NewPluginSchema(d.Plugin.SchemaMode)

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
				msg := fmt.Sprintf("excluding %s: schema mismatch", tableName)
				log.Println("[INFO] - ", msg)

				// TODO use standard codes/enum?
				logMessages[connectionName] = append(logMessages[connectionName], msg)
				// NOTE: remove this table from ALL entries in AggregatedTablesByConnection
				for _, tableMap := range d.AggregatedTablesByConnection {
					delete(tableMap, tableName)
				}
			}
		}
	}
}

// build map of the tables provided by each child connection
// this takes into account the table 'Aggregation' property and the aggregator config TableAggregationSpecs
func (d *ConnectionData) getAggregatedTablesByConnection(aggregatorConfig *proto.ConnectionConfig) (map[string][]string, error) {
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
		var exclusionReasons map[string]string
		d.AggregatedTablesByConnection[c], exclusionReasons = connectionData.getAggregatedTables(aggregatorConfig)
		if exclusionCount := len(exclusionReasons); exclusionCount > 0 {
			log.Printf("[INFO] Child connection %s excluded %d %s",
				c,
				exclusionCount,
				pluralize.NewClient().Pluralize("table", exclusionCount, false))

			for t, reason := range exclusionReasons {
				log.Printf("[INFO] - %s : %s", t, reason)
			}
		}
	}
	return logMessages, nil
}

func (d *ConnectionData) includeTableInAggregator(connectionName string, table *Table, aggregatorConfig *proto.ConnectionConfig) (include bool, reason string) {
	// see if there is a matching table spec
	if aggregationSpec := aggregatorConfig.GetAggregationSpecForTable(table.Name); aggregationSpec != nil {
		// if a table is matched by the connection config, this overrides any table defintion `Aggregation` props
		if !aggregationSpec.MatchesConnection(connectionName) {
			return false, fmt.Sprintf("excluded by aggregation spec: match: %s, connections: %s", aggregationSpec.Match, strings.Join(aggregationSpec.Connections, ","))
		}
		// if we match the spec, ignore the table aggregation mode and return true
		return true, ""
	}

	// otherwise if the config does not specify this table, fall back onto the table a`Aggregation` property
	if table.Aggregation == AggregationModeNone {
		return false, "table aggregation mode set to 'none'"
	}
	return true, ""
}

// build a map of tables to include in an aggregator using this connection
// take into account tables excluded by the table def AND by the connection config
func (d *ConnectionData) getAggregatedTables(aggregatorConfig *proto.ConnectionConfig) (map[string]*Table, map[string]string) {
	res := make(map[string]*Table)
	exclusionReasons := make(map[string]string)

	for name, table := range d.TableMap {
		if include, reason := d.includeTableInAggregator(d.Connection.Name, table, aggregatorConfig); include {
			res[name] = table
		} else {
			exclusionReasons[name] = reason
		}
	}
	return res, exclusionReasons
}

func (d *ConnectionData) tableSchemaSameForAllConnections(tableName string) (*proto.TableSchema, bool) {
	p := d.Plugin
	var exemplarSchema *proto.TableSchema
	for _, c := range p.ConnectionMap {
		// only interested in non-aggregator connections
		if c.isAggregator() {
			continue
		}
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

func (d *ConnectionData) setSchema(tableMap map[string]*Table, schema *grpc.PluginSchema) *ConnectionData {
	d.TableMap = tableMap
	d.Schema = schema
	// chainable
	return d
}

func (d *ConnectionData) isAggregator() bool {
	return len(d.config.ChildConnections) > 0
}

package plugin

import (
	"context"
	"fmt"
	"golang.org/x/exp/maps"
	"log"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/gertd/go-pluralize"
	"github.com/turbot/go-kit/filewatcher"
	"github.com/turbot/steampipe-plugin-sdk/v5/getter"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// ConnectionData is the data stored by the plugin which is connection dependent.
type ConnectionData struct {
	// map of all the tables in the connection schema, keyed by the table name
	TableMap map[string]*Table
	// Connection struct contains the _parsed_ connection config object as an interface
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

func (d *ConnectionData) updateWatchPaths(watchPaths []string, p *Plugin) error {
	// close any existing watcher
	if d.Watcher != nil {
		log.Printf("[TRACE] ConnectionData updateWatchPaths - close existing watcher")
		d.Watcher.Close()
		d.Watcher = nil
	}

	// create WatcherOptions
	connTempDir := getConnectionTempDir(p.tempDir, d.Connection.Name)
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
	logMessages, err = d.setAggregatedTablesByConnection(aggregatorConfig)
	if err != nil {
		return logMessages, err
	}

	// resolve tables to include by comparing the table schemas for each connection
	// and resolving the aggregator schema, (based on the `Aggregation` property)
	d.resolveAggregatorTableMap(aggregatorConfig, logMessages)

	// log out the schema init process
	// this is very verbose
	//d.logInitAggregatorSchema(aggregatorConfig)

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

// for each table in AggregatedTablesByConnection, verify all connections have the key columns, and if so,
// build a superset schema and add to table map
func (d *ConnectionData) resolveAggregatorTableMap(aggregatorConfig *proto.ConnectionConfig, logMessages map[string][]string) {
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

			// try to build a superset schema for this table
			// If the tables have a different key columns connections this table will be EXCLUDED
			tableSchema, messages := d.buildAggregatorTableSchema(aggregatorConfig, tableName)
			if tableSchema != nil {
				// so we managed to build a schema
				d.TableMap[tableName] = table
				d.Schema.Schema[tableName] = tableSchema
			} else {
				// no schema returned - we are excluding this table
				log.Printf("[INFO] excluding %s", tableName)
				// NOTE: remove this table from ALL entries in AggregatedTablesByConnection
				for _, tableMap := range d.AggregatedTablesByConnection {
					delete(tableMap, tableName)
				}
			}
			// there may be messages even if we do not exclude the table
			if msgCount := len(messages); msgCount > 0 {
				log.Printf("[INFO] connection %s, table %s: %d schema build %s",
					connectionName,
					tableName,
					msgCount,
					pluralize.NewClient().Pluralize("message", msgCount, false))

				for _, m := range messages {
					log.Println("[INFO]", m)
				}

				logMessages[connectionName] = append(logMessages[connectionName], messages...)
			}
		}
	}
}

// build map of the tables provided by each child connection
func (d *ConnectionData) setAggregatedTablesByConnection(aggregatorConfig *proto.ConnectionConfig) (map[string][]string, error) {
	d.AggregatedTablesByConnection = make(map[string]map[string]*Table)
	logMessages := make(map[string][]string)
	for _, c := range aggregatorConfig.ChildConnections {
		// find connection data for this child connection
		connectionData, ok := d.Plugin.getConnectionData(c)
		if !ok {
			// should never happen
			return nil, fmt.Errorf("initAggregatorSchema failed for aggregator connection '%s': no connection data loaded for child connection '%s'", aggregatorConfig.Connection, c)
		}

		// ask the connection data to build a list of tables that this connection provides
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
	// TODO for now hard code to true
	// update when (if)_ we re-add aggregation specs
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

func (d *ConnectionData) buildAggregatorTableSchema(aggregatorConfig *proto.ConnectionConfig, tableName string) (*proto.TableSchema, []string) {
	exemplarSchema, connectionTableDiffs, messages := d.getSchemaDiffBetweenConnections(aggregatorConfig, tableName)

	// if there is no exemplar schema, or there are no diffs, there is nothing more to do
	if exemplarSchema == nil || !connectionTableDiffs.HasDiffs() {
		return exemplarSchema, messages
	}

	// so there are diffs between the schemas for this table for each connection

	//  build a superset schema
	var superset = &proto.TableSchema{
		Description:                exemplarSchema.Description,
		GetCallKeyColumns:          exemplarSchema.GetCallKeyColumns,
		ListCallKeyColumns:         exemplarSchema.ListCallKeyColumns,
		ListCallOptionalKeyColumns: exemplarSchema.ListCallOptionalKeyColumns,
		GetCallKeyColumnList:       exemplarSchema.GetCallKeyColumnList,
		ListCallKeyColumnList:      exemplarSchema.ListCallKeyColumnList,
	}

	includedColumns := make(map[string]struct{})
	for _, connectionName := range aggregatorConfig.ChildConnections {
		c, ok := d.Plugin.getConnectionData(connectionName)
		if !ok {
			// unexpected
			log.Printf("[WARN] buildAggregatorTableSchema: connection '%s' (child of aggregator '%s') has no connectionData stored", connectionName, aggregatorConfig.Connection)
			continue
		}
		tableSchema, ok := c.Schema.Schema[tableName]
		if !ok {
			continue
		}

		for _, column := range tableSchema.Columns {
			// have we already handled this column
			if _, ok := includedColumns[column.Name]; ok {
				continue
			}

			if _, columnTypeMismatch := connectionTableDiffs.TypeMismatchColumns[column.Name]; columnTypeMismatch {
				// set the column type to JSON
				// overwrite 'column'
				column = &proto.ColumnDefinition{
					Name:        column.Name,
					Type:        proto.ColumnType_JSON,
					Description: column.Description,
				}
			}

			// ok including this column
			superset.Columns = append(superset.Columns, column)
			includedColumns[column.Name] = struct{}{}

			// check whether this column is a connectionKeyColumn and if so, create get and list key columns for the superset schema
			d.addConnectionKeyColumns(column, superset)
		}
	}

	return superset, messages
}

// add key columns for the `sp_connection_name` column, as well as any other connection key columns defined by the plugin
func (d *ConnectionData) addConnectionKeyColumns(column *proto.ColumnDefinition, superset *proto.TableSchema) {
	_, isConnectionKeyColumn := d.Plugin.ConnectionKeyColumns[column.Name]
	if column.Name == connectionNameColumnName || isConnectionKeyColumn {
		d.addKeyColumn(column, superset)
	}
}

func (d *ConnectionData) addKeyColumn(column *proto.ColumnDefinition, superset *proto.TableSchema) {
	// add to the get and list key columns
	kc := &proto.KeyColumn{
		Name: column.Name,
		// todo like?
		Operators: []string{"="},
		Require:   Optional,
	}
	// check whether we already have a key column for this column
	for _, k := range superset.GetCallKeyColumnList {
		if k.Name == column.Name {
			break
		}

		// no get key column - add one
		superset.GetCallKeyColumnList = append(superset.GetCallKeyColumnList, kc)
	}
	for _, k := range superset.ListCallKeyColumnList {
		if k.Name == column.Name {
			break
		}

		// no list key column - add one
		superset.ListCallKeyColumnList = append(superset.ListCallKeyColumnList, kc)
	}
}

func (d *ConnectionData) getSchemaDiffBetweenConnections(aggregatorConfig *proto.ConnectionConfig, tableName string) (*proto.TableSchema, *proto.TableSchemaDiff, []string) {
	// construct a diff for all connections
	var connectionTableDiffs = proto.NewTableSchemaDiff()

	var messages []string

	var exemplarSchema *proto.TableSchema
	for _, connectionName := range aggregatorConfig.ChildConnections {
		c, ok := d.Plugin.getConnectionData(connectionName)
		if !ok {
			log.Printf("[WARN] buildAggregatorTableSchema: connection '%s' (child of aggregator %s) has no connectionData stored", connectionName, aggregatorConfig.Connection)
			continue
		}
		// does this connection have this table?
		tableSchema, ok := c.Schema.Schema[tableName]
		if !ok || len(tableSchema.Columns) == 0 {
			log.Printf("[TRACE] buildAggregatorTableSchema: connection %s does not provide table %s", c.Connection.Name, tableName)
			continue
		}
		if exemplarSchema == nil {
			exemplarSchema = tableSchema
			continue
		}

		schemaDiff := tableSchema.Diff(exemplarSchema)

		// if key columns not equal, fail immediately
		if !schemaDiff.KeyColumnsEqual {
			messages = append(messages, fmt.Sprintf("cannot build merged schema for table %s as key columns are not consistent", tableName))
			return nil, nil, messages
		}

		// merge the diffs
		connectionTableDiffs.Merge(schemaDiff)
	}

	return exemplarSchema, connectionTableDiffs, messages
}

func (d *ConnectionData) setSchema(tableMap map[string]*Table, schema *grpc.PluginSchema) {
	d.TableMap = tableMap
	d.Schema = schema
}

func (d *ConnectionData) isAggregator() bool {
	return d.config.IsAggregator()

}

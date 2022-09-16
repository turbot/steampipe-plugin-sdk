/*
	Package steampipe_plugin_sdk makes writing [Steampipe] plugins simple.

# Create the Plugin Definition

By convention the Go files for your plugin (except main.go) should reside in a folder with the same name as the plugin.

Create the file <plugin-name>/plugin.go.
Implement a function that creates a plugin definition of type [plugin.Plugin], and returns a pointer to it.

Examples:
  - [aws plugin definition]
  - [zendesk plugin definition]

# Create the Plugin Entry Point

Create main.go in your plugin root directory. Add a main function, which is the entry point for your plugin.

This function must call [plugin.Serve] to instantiate your plugin's gRPC server.
It should pass the plugin creation function you just wrote

	package main

	import (
		"github.com/turbot/steampipe-plugin-aws/aws"
		"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
	)

	func main() {
		plugin.Serve(&plugin.ServeOpts{
			PluginFunc: aws.Plugin})
	}

Examples:
  - [aws main]
  - [zendesk main]

# Add Your First Table Definition

By convention, each table should be implemented in a separate file named table_<table name>.go.  Each table will have a single table definition function that returns a pointer to a [plugin.Table].

The table definition includes the name and description of the table, a list of column definitions, and the functions to call in order to list the data for all the rows, or to get data for a single row.

Use [plugin.Table] to define a table.

# Add a List Call

This is a function of type [plugin.HydrateFunc] which calls your API to return all rows in the table.
To add, set the property [plugin.Plugin.ListConfig]

# Add a Get Call

If the API supports returning a single item keyed by id, implement a Get call
To add, set [plugin.Plugin.GetConfig]

# Add Column Definitions

Use [plugin.Column] to define columns.

# Add Hydrate Functions

A column may be populated by a List or Get call.
It may alternatively define a [plugin.HydrateFunc] that makes an additional API call for each row of the table to populate the column.

Add a hydrate function for a column by setting [plugin.Column.Hydrate].

# Add Transform Functions

Use [transform] functions to extract and/or reformat data returned by the hydrate functions.

# Logging

A logger is passed to the plugin via the context.  You can use the logger to write messages to the log at standard log levels:

	logger := plugin.Logger(ctx)
	logger.Info("Log message and a variable", myVariable)

The plugin logs are not written to the console, but are written to the plugin logs at ~/.steampipe/logs/plugin-YYYY-MM-DD.log, e.g. ~/.steampipe/logs/plugin-2022-01-01.log.

Steampipe uses https://github.com/hashicorp/go-hclog hclog, which uses standard log levels (TRACE, DEBUG, INFO, WARN, ERROR).
By default, the log level is WARN.  You set it using the STEAMPIPE_LOG_LEVEL environment variable:

	export STEAMPIPE_LOG_LEVEL=TRACE

# Set Error Handling behaviour (advanced)

(TODO)

# Set Hydrate Dependencies (advanced)

Steampipe parallelizes hydrate functions as much as possible. Sometimes, however, one hydrate function requires the output from another.  Use [plugin.HydrateConfig] to define the dependency.

	return &plugin.Table{
			Name: "hydrate_columns_dependency",
			List: &plugin.ListConfig{
				Hydrate: hydrateList,
			},
			HydrateConfig: []plugin.HydrateConfig{
				{
					Func:    hydrate2,
					Depends: []plugin.HydrateFunc{hydrate1},
				},
			},
			Columns: []*plugin.Column{
				{Name: "id", Type: proto.ColumnType_INT},
				{Name: "hydrate_column_1", Type: proto.ColumnType_STRING, Hydrate: hydrate1},
				{Name: "hydrate_column_2", Type: proto.ColumnType_STRING, Hydrate: hydrate2},
			},
		}

Here, hydrate function hydrate2 is dependent on hydrate1. This means hydrate2 will not execute until hydrate1 has completed and the results are available. hydrate2 can refer to the results from hydrate1 as follows:

	func hydrate2(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
			// NOTE: in this case we know the output of hydrate1 is map[string]interface{} so we cast it accordingly.
			// the data should be cast to th appropriate type
		hydrate1Results := h.HydrateResults["hydrate1"].(map[string]interface{})
	.....
	}

Note that:
  - Multiple dependencies are supported.
  - Circular dependencies will be detected and cause a validation failure.
  - The Get and List hydrate functions ***CANNOT*** have dependencies.

# Dynamic Tables (advanced)

Use a plugin [dynamic_tables] when you cannot know a table's schema in advance, e.g. the [CSV plugin].

# Flow of execution

  - A user runs a query.
  - Postgres parses the query and sends the parsed request to the FDW.
  - The FDW determines which tables and columns are required.
  - The FDW calls one or more [HydrateFunc] to fetch API data.
  - Each table defines special hydrate functions: List and optionally Get. These will always be called before any other hydrate function in the table, as the other functions typically depend on the List or Get.
  - One or more [transform] functions are called for each column. These extract and/or reformat data returned by the hydrate functions.
  - The plugin returns the transformed data to the FDW.
  - Steampipe FDW returns the results to the database.

[aws main]: https://github.com/turbot/steampipe-plugin-aws/blob/main/main.go
[zendesk main]: https://github.com/turbot/steampipe-plugin-zendesk/blob/main/main.go
[aws plugin definition]: https://github.com/turbot/steampipe-plugin-aws/blob/main/aws/plugin.go
[zendesk plugin definition]: https://github.com/turbot/steampipe-plugin-zendesk/blob/zendesk/plugin.go
[CSV plugin]: https://hub.steampipe.io/plugins/turbot/csv/tables/%7Bcsv_filename%7D

[Steampipe]: https://github.com/turbot/steampipe
*/
package steampipe_plugin_sdk

import (
	"github.com/turbot/steampipe-plugin-sdk/v4/docs/dynamic_tables"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/transform"
)

var forceImportDynamicPlugin dynamic_tables.ForceImport
var forceImportPlugin plugin.ForceImport
var forceImportTransform transform.ForceImport

// package main is the main package
/*
Plugin is the top-level package of the [Steampipe] plugin SDK. It provides data structures and functions that enable a plugin to read data from an API and stream it into Postgres tables by way of Steampipe's [foreign data wrapper] (FDW).

[Steampipe]: https://github.com/turbot/steampipe


# Flow of execution

  - A user runs a query.
  - Postgres parses the query and sends the parsed request to the FDW.
  - The FDW determines which tables and columns are required.
  - The FDW calls one or more [HydrateFunc] to fetch API data.
  - Each table defines special hydrate functions: `List` and optionally `Get`. These will always be called before any other hydrate function in the table, as the other functions typically depend on the `List` or `Get`.
  - One or more [transform] functions are called for each column. These extract and/or reformat data returned by the hydrate functions.
  - The plugin returns the transformed data to the FDW.
  - Steampipe FDW returns the results to the database.

# main.go

The `main` function in `main.go` is the entry point for your plugin.  This function must call [plugin.Serve] to instantiate your plugin's gRPC server.

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
  - [aws]
  - [zendesk]

[aws]: https://github.com/turbot/steampipe-plugin-aws/blob/main/main.go]
[zendesk]: https://github.com/turbot/steampipe-plugin-zendesk/blob/main/main.go]


# plugin.go

`plugin.go` implements a function that returns a pointer to the [plugin.Plugin] loaded by Steampipe's [plugin manager].

[plugin manager]: https://github.com/turbot/steampipe/tree/main/pluginmanager

By convention, the package name for your plugin should be the same name as your plugin, and the Go files for your plugin (except `main.go`) should reside in a folder with the same name.

# Plugin definition

Use [plugin.Plugin] to define a plugin.

# Table definition

By convention, each table should be implemented in a separate file named `table_{table name}.go`.  Each table will have a single table definition function that returns a pointer to a [plugin.Table].

The table definition includes the name and description of the table, a list of column definitions, and the functions to call in order to list the data for all the rows, or to get data for a single row.

Use [plugin.Table] to define a table.

# Column definition

A column may be populated by a List or Get call. It may alternatively define its own [HydrateFunc] that makes an additional API call for each row. Use [plugin.Column] to define a column.

# Hydrate functions

Use [HydrateFunc] to call an API and return data.

# ListConfig

Use [plugin.ListConfig] to define a List call.


# GetConfig

Use [plugin.GetConfig] to define a Get call.

# HydrateConfig

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

Here, hydrate function `hydrate2` is dependent on `hydrate1`. This means `hydrate2` will not execute until `hydrate1` has completed and the results are available. `hydrate2` can refer to the results from `hydrate1` as follows:

	func hydrate2(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
			// NOTE: in this case we know the output of hydrate1 is map[string]interface{} so we cast it accordingly.
			// the data should be cast to th appropriate type
		hydrate1Results := h.HydrateResults["hydrate1"].(map[string]interface{})
	.....
	}

Note that:
  - Multiple dependencies are supported.
  - Circular dependencies will be detected and cause a validation failure.
  - The `Get` and `List` hydrate functions ***CANNOT*** have dependencies.

# Transform functions

Use [transform] functions to extract and/or reformat data returned by the hydrate functions.

# Dynamic plugin

Use [dynamic_plugin] when you cannot know a table's schema in advance, e.g. the [CSV plugin].

[CSV plugin]: https://hub.steampipe.io/plugins/turbot/csv/tables/%7Bcsv_filename%7D

# Logging

A logger is passed to the plugin via the context.  You can use the logger to write messages to the log at standard log levels:

	logger := plugin.Logger(ctx)
	logger.Info("Log message and a variable", myVariable)

The plugin logs do not currently get written to the console, but are written to the plugin logs at `~/.steampipe/logs/plugin-YYYY-MM-DD.log`, e.g., `~/.steampipe/logs/plugin-2022-01-01.log`.

Steampipe uses https://github.com/hashicorp/go-hclog hclog, which uses standard log levels (`TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`). By default, the log level is `WARN`.  You set it using the `STEAMPIPE_LOG_LEVEL` environment variable:

	export STEAMPIPE_LOG_LEVEL=TRACE

*/
package main

import (
	"github.com/turbot/steampipe-plugin-sdk/v4/docs/dynamic_plugin"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/transform"
)

var forceImportDynamicPlugin dynamic_plugin.ForceImport
var forceImportPlugin plugin.ForceImport
var forceImportTransform transform.ForceImport

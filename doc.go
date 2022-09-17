/*
	Package steampipe_plugin_sdk makes it easy to write Steampipe plugins.

# Define the plugin

Create the file <plugin-name>/plugin.go.

Implement a [plugin.PluginFunc] that creates a [plugin.Plugin] and returns a pointer to it.

Note: The Go files for your plugin (except main.go) should reside in the <plugin-name> folder.

Examples:
  - [aws plugin.go]
  - [zendesk plugin.go]

# Create the plugin entry point

Create main.go in your plugin's root directory. Add a main function which is the entry point for your plugin.

This function must call [plugin.Serve] to instantiate your plugin's gRPC server, and pass the [plugin.PluginFunc] that you just wrote.

	package main

	import (
		"github.com/turbot/steampipe-plugin-aws/aws"
		"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	)

	func main() {
		plugin.Serve(&plugin.ServeOpts{
			PluginFunc: aws.Plugin})
	}

Examples:
  - [aws main.go]
  - [zendesk main.go]

# Define your first table

By convention, each table lives in a separate file named table_<table name>.go.  Each table has a single table definition function that returns a pointer to a [plugin.Table].

The table definition includes the name and description of the table, a set of column definitions, and the functions to call in order to list the data for all the rows, or to get data for a single row.

Every table MUST define a List and/or Get function.

	func tableZendeskTicket() *plugin.Table {
		return &plugin.Table{
			Name:        "zendesk_ticket",
			Description: "Tickets enable your customers to communicate with agents in Zendesk Support.",
			List: &plugin.ListConfig{
				Hydrate: listTicket,
			},
			Get: &plugin.GetConfig{
				KeyColumns: plugin.SingleColumn("id"),
				Hydrate:    getTicket,
			},
			Columns: []*plugin.Column{
				{
					Name: "allow_attachments",
					Type: proto.ColumnType_BOOL,
					Description: "Permission for agents to add add attachments to a comment. Defaults to true"
				},
				...
				{
					Name: "via_source_to",
					Type: proto.ColumnType_JSON,
					Transform: transform.FromField("Via.Source.From"),
					Description: "Target that received the ticket"
				},
			},
		}
	}

Examples:
  - [aws table_aws_ec2_instance.go]
  - [zendesk table_zendesk_ticket.go]

# Define a List function

This is a [plugin.HydrateFunc] that calls an API and returns data for all rows for the table.

To define it, set the property [plugin.Plugin.ListConfig].

	List: &plugin.ListConfig{
		Hydrate: listTicket,
	},

# Define a Get function

This is a [plugin.HydrateFunc] that calls an API and returns data for one row of the table.
If the API can return a single item keyed by id, you should implement Get so that queries can filter the data as cheaply as possible.

To define it, set the property [plugin.Plugin.GetConfig].

	Get: &plugin.GetConfig{
		KeyColumns: plugin.SingleColumn("id"),
		Hydrate:    getTicket,
	},

# Add column definitions

Use [plugin.Column] to define columns.

# Add hydrate functions

A column may be populated by a List or Get call. If a column requires data not provide by List or Get, it may define a [plugin.HydrateFunc] that makes an additional API call for each row.

Add a hydrate function for a column by setting [plugin.Column.Hydrate].

# Add transform functions

Use [transform] functions to extract and/or reformat data returned by a hydrate function.

# Logging

A [plugin.Logger] is passed to the plugin via its [context.Context]. Messages are written to ~/.steampipe/logs, e.g. ~/.steampipe/logs/plugin-2022-01-01.log.

Steampipe uses [go-hclog] which supports standard log levels: TRACE, DEBUG, INFO, WARN, ERROR. The default is WARN.

	logger := plugin.Logger(ctx)
	logger.Info("Log message and a variable", myVariable)

Use the STEAMPIPE_LOG_LEVEL environment variable to set the level.

	export STEAMPIPE_LOG_LEVEL=TRACE

# Define hydrate dependencies (advanced)

Steampipe parallelizes hydrate functions as much as possible. Sometimes, however, one hydrate function requires the output from another.  Use [plugin.HydrateConfig] to define the dependency.

# Dynamic tables (advanced)

Use [dynamic_tables] when you cannot know a table's schema in advance, e.g. the [CSV plugin].

# Flow of execution

  - A user runs a query.

  - Postgres parses the query and sends the parsed request to the Steampipe [foreign data wrapper] (FDW).

  - The FDW determines which tables and columns are required.

  - The FDW calls one or more [HydrateFunc] to fetch API data.

  - Each table defines special hydrate functions: List and optionally Get. These will always be called before any other hydrate function in the table, as the other functions typically depend on the List or Get.

  - One or more [transform] functions are called for each column. These extract and/or reformat data returned by the hydrate functions.

  - The plugin returns the transformed data to the FDW.

  - Steampipe FDW returns the results to the database.

[aws plugin.go]: https://github.com/turbot/steampipe-plugin-aws/blob/main/aws/plugin.go
[zendesk plugin.go]: https://github.com/turbot/steampipe-plugin-zendesk/blob/main/zendesk/plugin.go
[aws main.go]: https://github.com/turbot/steampipe-plugin-aws/blob/main/main.go
[zendesk main.go]: https://github.com/turbot/steampipe-plugin-zendesk/blob/main/main.go
[aws table_aws_ec2_instance.go]: https://github.com/turbot/steampipe-plugin-aws/blob/main/aws/table_aws_ec2_instance.go
[zendesk table_zendesk_ticket.go]: https://github.com/turbot/steampipe-plugin-zendesk/blob/main/zendesk/table_zendesk_ticket.go
[go-hclog]: https://github.com/hashicorp/go-hclog
[CSV plugin]: https://hub.steampipe.io/plugins/turbot/csv/tables/%7Bcsv_filename%7D
[foreign data wrapper]: https://github.com/turbot/steampipe-postgres-fdw
*/
package steampipe_plugin_sdk

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/docs/dynamic_tables"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

var forceImportDynamicPlugin dynamic_tables.ForceImport
var forceImportPlugin plugin.ForceImport
var forceImportTransform transform.ForceImport

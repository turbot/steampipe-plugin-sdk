/*
Plugin is the top-level package of the [Steampipe] plugin SDK. It provides data structures and functions that enable a plugin to read data from an API and stream it into Postgres tables by way of Steampipe's [foreign data wrapper] (FDW).
 
[Steampipe]: https://github.com/turbot/steampipe
[foreign data wrapper]: https://github.com/turbot/steampipe-postgres-fdw

# Flow of execution

	- A user runs a steampipe query against the database.
	- Postgres parses the query and sends the parsed request to the FDW.
	- The FDW determines which tables and columns are required.
	- The FDW calls the appropriate [HydrateFunc] in the plugin; these functions fetch data from the APIs.
    - Each table defines two special hydrate functions, `List` and `Get`, defined by [plugin.ListConfig] and [plugin.GetConfig].  The `List` or `Get` will always be called before any other hydrate function in the table, as the other functions typically depend on the result of the Get or List call. 
	- The [transform] functions are called for each column. These extract and/or reformat data returned by the hydrate functions.
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

	package zendesk

	import (
		"context"

		"github.com/turbot/steampipe-plugin-sdk/plugin"
		"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
	)

	func Plugin(ctx context.Context) *plugin.Plugin {
		p := &plugin.Plugin{
			Name:             "steampipe-plugin-zendesk",
			DefaultTransform: transform.FromGo().NullIfZero(),
			TableMap: map[string]*plugin.Table{
				"zendesk_brand":        tableZendeskBrand(),
				"zendesk_group":        tableZendeskGroup(),
				"zendesk_organization": tableZendeskOrganization(),
				"zendesk_search":       tableZendeskSearch(),
				"zendesk_ticket":       tableZendeskTicket(),
				"zendesk_ticket_audit": tableZendeskTicketAudit(),
				"zendesk_trigger":      tableZendeskTrigger(),
				"zendesk_user":         tableZendeskUser(),
			},
		}
		return p
	}

Examples:
	- [aws]
	- [zendesk]

[aws]: https://github.com/turbot/steampipe-plugin-aws/blob/main/main.go]
[zendesk]: https://github.com/turbot/steampipe-plugin-zendesk/blob/main/zendesk/plugin.go

# Table definition

By convention, each table should be implemented in a separate file named `table_{table name}.go`.  Each table will have a single table definition function that returns a pointer to a [plugin.Table].

The table definition includes the name and description of the table, a list of column definitions, and the functions to call in order to list the data for all the rows, or to get data for a single row.

	package zendesk

	import (
		"context"

		"github.com/nukosuke/go-zendesk/zendesk"

		"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
		"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
		"github.com/turbot/steampipe-plugin-sdk/v4/plugin/transform"
	)

	func tableZendeskTicket() *plugin.Table {
		return &plugin.Table{
			Name:        "zendesk_ticket",
			Description: "Tickets are the means through which your end users (customers) communicate with agents in Zendesk Support.",
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
					Name: "via_source_ref", 
					Type: proto.ColumnType_STRING, 
					Transform: transform.FromField("Via.Source.Ref"), 
					Description: "Medium used to raise the ticket"
				},
			},
		}
	}

Examples:
	- [aws]
	- [zendesk]

[aws]: https://github.com/turbot/steampipe-plugin-aws/blob/main/aws/table_aws_ec2_ami.go
[zendesk]: https://github.com/turbot/steampipe-plugin-zendesk/blob/main/zendesk/table_zendesk_ticket.go


[Steampipe connection]: https://steampipe.io/docs/managing/connections

# Column definition

[plugin.Column]

# Hydrate functions

A [HydrateFunc] calls an API and returns data.

# List Config

[plugin.ListConfig]

# Get Config

[plugin.GetConfig]


# Hydrate dependencies

Steampipe parallelizes hydrate functions as much as possible. Sometimes, however, one hydrate function requires the output from another.  You can define [plugin.HydrateDependencies] for this case:

	return &plugin.Table{
			Name: "hydrate_columns_dependency",
			List: &plugin.ListConfig{
				Hydrate: hydrateList,
			},
			HydrateDependencies: []plugin.HydrateDependencies{
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

These extract and/or reformat data returned by the hydrate functions. See [transform].

# Dynamic tables

If [plugin.SchemaMode] is set to `dynamic`, every time
Steampipe starts the plugin's schema will be checked for any changes since the
last time it loaded, and re-import the schema if it detects any.

Dynamic tables are useful when you are building a plugin whose schema is not
known at compile time; instead, its schema will be generated at runtime. For
instance, a plugin with dynamic tables is useful if you want to load CSV files
as tables from one or more directories. Each of these CSV files may have
different column structures, resulting in a different structure for each table.

In order to create a dynamic table, [plugin.TableMapFunc]
should call a function that returns `map[string]*plugin.Table`.

	func Plugin(ctx context.Context) *plugin.Plugin {
		p := &plugin.Plugin{
			Name: "steampipe-plugin-csv",
			ConnectionConfigSchema: &plugin.ConnectionConfigSchema{
				NewInstance: ConfigInstance,
				Schema:      ConfigSchema,
			},
			DefaultTransform: transform.FromGo().NullIfZero(),
			SchemaMode:       plugin.SchemaModeDynamic,
			TableMapFunc:     PluginTables,
		}
		return p
	}

	func PluginTables(ctx context.Context, p *plugin.Plugin) (map[string]*plugin.Table, error) {
		// Initialize tables
		tables := map[string]*plugin.Table{}

		// Search for CSV files to create as tables
		paths, err := csvList(ctx, p)
		if err != nil {
			return nil, err
		}
		for _, i := range paths {
			tableCtx := context.WithValue(ctx, "path", i)
			base := filepath.Base(i)
		// tableCSV returns a *plugin.Table type
			tables[base[0:len(base)-len(filepath.Ext(base))]] = tableCSV(tableCtx, p)
		}

		return tables, nil
	}


The `tableCSV` function mentioned in the example above looks for all CSV files in the configured paths, and for each one, builds a `*plugin.Table` type:

	func tableCSV(ctx context.Context, p *plugin.Plugin) *plugin.Table {

		path := ctx.Value("path").(string)
		csvFile, err := os.Open(path)
		if err != nil {
			plugin.Logger(ctx).Error("Could not open CSV file", "path", path)
			panic(err)
		}

		r := csv.NewReader(csvFile)

		csvConfig := GetConfig(p.Connection)
		if csvConfig.Separator != nil && *csvConfig.Separator != "" {
			r.Comma = rune((*csvConfig.Separator)[0])
		}
		if csvConfig.Comment != nil {
			if *csvConfig.Comment == "" {
				// Disable comments
				r.Comment = 0
			} else {
				// Set the comment character
				r.Comment = rune((*csvConfig.Comment)[0])
			}
		}

		// Read the header to peak at the column names
		header, err := r.Read()
		if err != nil {
			plugin.Logger(ctx).Error("Error parsing CSV header:", "path", path, "header", header, "err", err)
			panic(err)
		}
		cols := []*plugin.Column{}
		for idx, i := range header {
			cols = append(cols, &plugin.Column{
				Name: i, 
				Type: proto.ColumnType_STRING, 
				Transform: transform.FromField(i), 
				Description: fmt.Sprintf("Field %d.", idx)
			})
		}

		return &plugin.Table{
			Name:        path,
			Description: fmt.Sprintf("CSV file at %s", path),
			List: &plugin.ListConfig{
				Hydrate: listCSVWithPath(path),
			},
			Columns: cols,
		}
	}

The end result is that, when using the CSV plugin, whenever Steampipe starts it will
check for any new, deleted, and modified CSV files in the configured `paths`
and create any discovered CSVs as tables. The CSV filenames are turned directly
into table names.

For more information on how the CSV plugin can be queried as a result of being
a dynamic table, please see https://hub.steampipe.io/plugins/turbot/csv/tables/%7Bcsv_filename%7D

# Logging

A logger is passed to the plugin via the context.  You can use the logger to write messages to the log at standard log levels:

	logger := plugin.Logger(ctx)
	logger.Info("Log message and a variable", myVariable)

The plugin logs do not currently get written to the console, but are written to the plugin logs at `~/.steampipe/logs/plugin-YYYY-MM-DD.log`, e.g., `~/.steampipe/logs/plugin-2022-01-01.log`.

Steampipe uses https://github.com/hashicorp/go-hclog hclog, which uses standard log levels (`TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`). By default, the log level is `WARN`.  You set it using the `STEAMPIPE_LOG_LEVEL` environment variable:

	export STEAMPIPE_LOG_LEVEL=TRACE

*/
package plugin

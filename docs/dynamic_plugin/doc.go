/*
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
a dynamic table, please see the [CSV plugin]

[CSV plugin]: https://hub.steampipe.io/plugins/turbot/csv/tables/%7Bcsv_filename%7D
*/
package dynamic_plugin

type ForceImport string

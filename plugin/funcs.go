package plugin

import "context"

/*
HydrateFunc is a function that gathers data to build table rows.
Typically this would make an API call and return the raw API output.

List and Get are special hydrate functions.

  - List returns data for all rows. Almost all tables will have a List function.

  - Get returns data for a single row. In order to filter as cheaply as possible a Get function should be implemented if 		the API supports fetching single items by key.

A column may require data not returned by the List or Get calls and an additional API
call will be required. A HydrateFunc that wraps this API call can be specified in the [Column] definition.

You could do this the hard way by looping through the List API results and enriching each item
by making an additional API call. However the SDK does all this for you.
*/
type HydrateFunc func(context.Context, *QueryData, *HydrateData) (interface{}, error)

/*
Deprecated
*/
type MatrixItemFunc func(context.Context, *Connection) []map[string]interface{}

/*
MatrixItemMapFunc is a callback function which may be implemented by the plugin to provide a map of [matrix_items] to execute the query with.

[matrix_items] are a powerful way of executing the same query multiple times in parallel for a set of parameters.

Plugin examples:

  - Declaration of [MatrixItemMapFunc] and its [implementation].

[MatrixItemMapFunc]: https://github.com/turbot/steampipe-plugin-aws/blob/c5fbf38df19667f60877c860cf8ad39816ff658f/aws/table_aws_acm_certificate.go#L36
[implementation]: https://github.com/turbot/steampipe-plugin-aws/blob/c5fbf38df19667f60877c860cf8ad39816ff658f/aws/multi_region.go#L63
*/
type MatrixItemMapFunc func(context.Context, *QueryData) []map[string]interface{}

//ErrorPredicate is a function type which accepts error as an input and returns a boolean value.
type ErrorPredicate func(error) bool

/*
ErrorPredicateWithContext is a function type which accepts context, query data and hydrate data and error as an input and returns a boolean value.

Plugin examples:
  - [aws]
  - [azure]

[aws]: https://github.com/turbot/steampipe-plugin-aws/blob/010ec0762c273b4549b4369fe05d61ec1ce24a9b/aws/errors.go#L14
[azure]: https://github.com/turbot/steampipe-plugin-azure/blob/85d6f373f85726a9f045f907509d9fd82ace9e41/azure/errors.go#L11
*/
type ErrorPredicateWithContext func(context.Context, *QueryData, *HydrateData, error) bool

// TableMapFunc is callback function which can be used to populate [plugin.Plugin.TableMap]
// and allows the connection config to be used in the table creation
// (connection config is not available at plugin creation time)
//
// This callback function should be implementred by the plugin writer for dynamic plugins
type TableMapFunc func(ctx context.Context, connection *Connection) (map[string]*Table, error)

package plugin

import (
	"context"
)

/*
Deprecated use MatrixItemMapFunc
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

// ErrorPredicate is a function type which accepts error as an input and returns a boolean value.
type ErrorPredicate func(error) bool

/*
ErrorPredicateWithContext is a function type which accepts context, query data, hydrate data and error as an input and returns a boolean value.

Plugin examples:
  - [aws]
  - [azure]

[aws]: https://github.com/turbot/steampipe-plugin-aws/blob/010ec0762c273b4549b4369fe05d61ec1ce24a9b/aws/errors.go#L14
[azure]: https://github.com/turbot/steampipe-plugin-azure/blob/85d6f373f85726a9f045f907509d9fd82ace9e41/azure/errors.go#L11
*/
type ErrorPredicateWithContext func(context.Context, *QueryData, *HydrateData, error) bool

/*
TableMapFunc is callback function which can be used to populate [plugin.Plugin.TableMap]
and allows the connection config to be used in the table creation
(connection config is not available at plugin creation time).

This callback function should be implemented by the plugin writer for dynamic plugins.

Plugin examples:
  - [csv]

[csv]: https://github.com/turbot/steampipe-plugin-csv/blob/fa8c9809f4ebbfa2738e9ecd136da8b89a87f6eb/csv/plugin.go#L25
*/
type TableMapFunc func(ctx context.Context, d *TableMapData) (map[string]*Table, error)

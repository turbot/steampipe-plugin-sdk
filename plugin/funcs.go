package plugin

import "context"

// HydrateFunc is a function which retrieves some or all row data for a single row item.
type HydrateFunc func(context.Context, *QueryData, *HydrateData) (interface{}, error)

// deprecated
type MatrixItemFunc func(context.Context, *Connection) []map[string]interface{}

type MatrixItemMapFunc func(context.Context, *QueryData) []map[string]interface{}

type ErrorPredicate func(error) bool

type ErrorPredicateWithContext func(context.Context, *QueryData, *HydrateData, error) bool

// TableMapFunc is callback function which can be used to populate [plugin.Plugin.TableMap]
// and allows the connection config to be used in the table creation
// (connection config is not available at plugin creation time)
//
// This callback function should be implementred by the plugin writer for dynamic plugins
type TableMapFunc func(ctx context.Context, connection *Connection) (map[string]*Table, error)

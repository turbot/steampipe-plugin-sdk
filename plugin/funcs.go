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

// deprecated
type MatrixItemFunc func(context.Context, *Connection) []map[string]interface{}

/*
MatrixItemMapFunc provides a mechanism to query multiple matrix items instead of passing them individually in every [HydrateFunc].

It is cumbersome to define different set of regions every time a hydrate function is invoked. MatrixItemMapFunc helps you to define a set of regions which can execute the API calls parallely and then unify the results into different rows.
In certain cloud providers, region data needs to be passed into the [HydrateFunc] for execution. If we define
*/
type MatrixItemMapFunc func(context.Context, *QueryData) []map[string]interface{}

type ErrorPredicate func(error) bool

type ErrorPredicateWithContext func(context.Context, *QueryData, *HydrateData, error) bool

// TableMapFunc is callback function which can be used to populate [plugin.Plugin.TableMap]
// and allows the connection config to be used in the table creation
// (connection config is not available at plugin creation time)
//
// This callback function should be implementred by the plugin writer for dynamic plugins
type TableMapFunc func(ctx context.Context, connection *Connection) (map[string]*Table, error)

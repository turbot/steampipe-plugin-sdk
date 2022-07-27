package plugin

import "context"

// HydrateFunc is a function which retrieves some or all row data for a single row item.
type HydrateFunc func(context.Context, *QueryData, *HydrateData) (interface{}, error)

// deprecated
type MatrixItemFunc func(context.Context, *Connection) []map[string]interface{}

type MatrixItemMapFunc func(context.Context, *QueryData) []map[string]interface{}

type ErrorPredicate func(error) bool

type ErrorPredicateWithContext func(context.Context, *QueryData, *HydrateData, error) bool

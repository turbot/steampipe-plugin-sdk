package plugin

import "context"

// ConnectionColumnValuesFunc is a function which returns the  the values of all columns which have a
// 1-1 mapping with connection name.
// for example, if `account_id` column is always dependent on steampipe connection,
// this function will return the value of `account_id` for the given connection
type ConnectionColumnValuesFunc func(context.Context, *QueryData) (map[string]any, error)

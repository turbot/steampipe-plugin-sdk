package plugin

import (
	"context"
)

// HydrateFunc is a function which retrieves some or all row data for a single row item.
type HydrateFunc func(context.Context, *QueryData, *HydrateData) (interface{}, error)

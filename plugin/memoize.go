package plugin

import (
	"github.com/turbot/go-kit/helpers"
	"sync"
)

// map of currently executing memoized hydrate funcs

var memoizedHydrateFunctionsPending = make(map[string]*sync.WaitGroup)
var memoizedHydrateLock sync.RWMutex

/*
MemoizeHydrate ensures the [HydrateFunc] results are saved in the [connection.ConnectionCache].

Use it to reduce the number of API calls if the HydrateFunc is used by multiple tables.

MemoizeHydrate creates a memoized version of the supplied hydrate function and returns a NamedHydrateFunc
populated with the original function name.

This allow the plugin execution code to know the original function name, which is used to key the hydrate
function internally in the SDK.

# Usage

	{
		Name:        "account",
		Type:        proto.ColumnType_STRING,
		NamedHydrate:  plugin.Memoize(getCommonColumns)),
		Description: "The Snowflake account ID.",
		Transform:   transform.FromCamel(),
	}
*/
func MemoizeHydrate(hydrateFunc HydrateFunc, opts ...MemoizeOption) NamedHydrateFunc {
	memoized := hydrateFunc.Memoize(opts...)

	return NamedHydrateFunc{
		Func: memoized,
		// store the original function name
		Name: helpers.GetFunctionName(hydrateFunc),
	}
}

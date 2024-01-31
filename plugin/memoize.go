package plugin

import (
	"github.com/turbot/go-kit/helpers"
	"sync"
)

// map of currently executing memoized hydrate funcs

var memoizedHydrateFunctionsPending = make(map[string]*sync.WaitGroup)
var memoizedHydrateLock sync.RWMutex

/*
		MemoizeHydrate creates a memoized version of the supplied hydrate function and returns a NamedHydrateFunc
	    populated with the original function name.

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

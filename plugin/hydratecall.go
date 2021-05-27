package plugin

import (
	"context"

	"github.com/turbot/go-kit/helpers"
)

// HydrateData contains the input data passed to every hydrate function
type HydrateData struct {
	// if there was a parent-child list call, store the parent list item
	ParentItem     interface{}
	Item           interface{}
	HydrateResults map[string]interface{}
}

// HydrateFunc is a function which retrieves some or all row data for a single row item.
type HydrateFunc func(context.Context, *QueryData, *HydrateData) (interface{}, error)

// HydrateDependencies defines the hydrate function dependencies - other hydrate functions which must be run first
// Deprecated: used HydrateConfig
type HydrateDependencies struct {
	Func    HydrateFunc
	Depends []HydrateFunc
}

// HydrateConfig defines the hydrate function configurations, Name, Maximum number of concurrent calls to be allowed, dependencies
type HydrateConfig struct {
	Func              HydrateFunc
	MaxConcurrency    int
	RetryConfig       *RetryConfig
	ShouldIgnoreError ErrorPredicate
	Depends           []HydrateFunc
}

type RetryConfig struct {
	ShouldRetryError ErrorPredicate
}

// DefaultConcurrencyConfig contains plugin level config to define default hydrate concurrency
// - this is used if no HydrateConfig is specified for a specific call
type DefaultConcurrencyConfig struct {
	// max number of ALL hydrate calls in progress
	TotalMaxConcurrency   int
	DefaultMaxConcurrency int
}

// HydrateCall struct encapsulates a hydrate call, its config and dependencies
type HydrateCall struct {
	Func HydrateFunc
	// the dependencies expressed using function name
	Depends []string
	Config  *HydrateConfig
}

func newHydrateCall(hydrateFunc HydrateFunc, config *HydrateConfig) *HydrateCall {
	res := &HydrateCall{Func: hydrateFunc, Config: config}
	for _, f := range config.Depends {
		res.Depends = append(res.Depends, helpers.GetFunctionName(f))
	}
	return res
}

// CanStart returns whether this hydrate call can execute
// - check whether all dependency hydrate functions have been completed
// - check whether the concurrency limits would be exceeded

func (h HydrateCall) CanStart(rowData *RowData, name string, concurrencyManager *ConcurrencyManager) bool {
	for _, dep := range h.Depends {
		if !helpers.StringSliceContains(rowData.getHydrateKeys(), dep) {
			return false
		}
	}
	// ask the concurrency manager whether the call can start
	// NOTE: if the call is allowed to start, the concurrency manager ASSUMES THE CALL WILL START
	// and increments the counters
	// it may seem more logical to do this in the Start() function below, but we need to check and increment the counters
	// within the same mutex lock to ensure another call does not start between checking and starting
	return concurrencyManager.StartIfAllowed(name, h.Config.MaxConcurrency)
}

// Start starts a hydrate call
func (h *HydrateCall) Start(ctx context.Context, r *RowData, hydrateFuncName string, concurrencyManager *ConcurrencyManager) {
	// tell the rowdata to wait for this call to complete
	r.wg.Add(1)

	// call callHydrate async, ignoring return values
	go func() {
		r.callHydrate(ctx, r.queryData, h.Func, hydrateFuncName, h.Config.RetryConfig, h.Config.ShouldIgnoreError)
		// decrement number of hydrate functions running
		concurrencyManager.Finished(hydrateFuncName)
	}()
}

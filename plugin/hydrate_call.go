package plugin

import (
	"context"
	"sync/atomic"

	"github.com/turbot/go-kit/helpers"
)

// HydrateCall struct encapsulates a hydrate call, its config and dependencies
type HydrateCall struct {
	Func HydrateFunc
	// the dependencies expressed using function name
	Depends []string
	Config  *HydrateConfig
	Name    string
}

func newHydrateCall(hydrateFunc HydrateFunc, config *HydrateConfig) *HydrateCall {
	res := &HydrateCall{
		Name:   helpers.GetFunctionName(hydrateFunc),
		Func:   hydrateFunc,
		Config: config,
	}
	for _, f := range config.Depends {
		res.Depends = append(res.Depends, helpers.GetFunctionName(f))
	}
	return res
}

// CanStart returns whether this hydrate call can execute
// - check whether all dependency hydrate functions have been completed
// - check whether the concurrency limits would be exceeded
func (h HydrateCall) CanStart(rowData *RowData, name string, concurrencyManager *concurrencyManager) bool {
	// check whether all hydrate functions we depend on have saved their results
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
func (h *HydrateCall) Start(ctx context.Context, r *RowData, d *QueryData, concurrencyManager *concurrencyManager) {
	// tell the rowdata to wait for this call to complete
	r.wg.Add(1)
	// update the hydrate count
	atomic.AddInt64(&d.QueryStatus.hydrateCalls, 1)

	// call callHydrate async, ignoring return values
	go func() {
		r.callHydrate(ctx, d, h.Func, h.Name, h.Config)
		// decrement number of hydrate functions running
		concurrencyManager.Finished(h.Name)
	}()
}

package plugin

import (
	"context"

	"github.com/turbot/go-kit/helpers"
)

var concurrencyManager *ConcurrencyManager

func init() {
	concurrencyManager = newConcurrencyManager()
}

type HydrateData struct {
	Item           interface{}
	Params         map[string]string
	HydrateResults map[string]interface{}
}

// perform shallow clone
func (h *HydrateData) Clone() *HydrateData {
	return &HydrateData{
		Item:           h.Item,
		Params:         h.Params,
		HydrateResults: h.HydrateResults,
	}
}

// HydrateFunc is a function which retrieves some or all row data for a single row item.
type HydrateFunc func(context.Context, *QueryData, *HydrateData) (interface{}, error)

// HydrateDependencies :: define the hydrate function dependencies - other hydrate functions which must be run first
// DEPRECATED used HydrateConfig
type HydrateDependencies struct {
	Func    HydrateFunc
	Depends []HydrateFunc
}

// HydrateDependencies :: define the hydrate function dependencies - other hydrate functions which must be run first
type HydrateConfig struct {
	Func           HydrateFunc
	MaxConcurrency int
	// ConcurrencyMapKey ConcurrencyMapKeyFunc
	//ShouldRetryError ErrorPredicate
	//ShouldIgnoreError ErrorPredicate
	Depends []HydrateFunc
}

type DefaultHydrateConfig struct {
	// max number of ALL hydrate calls in progress
	MaxConcurrency int
}

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

// CanStart :: can this hydrate call - check whether all dependency hydrate functions have been completed
func (h HydrateCall) CanStart(rowData *RowData, name string) bool {
	for _, dep := range h.Depends {
		if !helpers.StringSliceContains(rowData.getHydrateKeys(), dep) {
			return false
		}
	}
	return concurrencyManager.StartIfAllowed(name, h.Config.MaxConcurrency)
}

func (h *HydrateCall) Start(ctx context.Context, r *RowData, hydrateFuncName string) {
	r.wg.Add(1)

	// call callHydrate async, ignoring return values
	go func() {
		r.callHydrate(ctx, r.queryData, h.Func, hydrateFuncName)
		// decrement number of hydrate functions running
		concurrencyManager.Finished(hydrateFuncName)
	}()
}

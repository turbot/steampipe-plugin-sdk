package plugin

import (
	"context"

	"github.com/turbot/go-kit/helpers"
)

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

var con ConcurrencyManager

// HydrateFunc is a function which retrieves some or all row data for a single row item.
type HydrateFunc func(context.Context, *QueryData, *HydrateData) (interface{}, error)

// HydrateDependencies :: define the hydrate function dependencies - other hydrate functions which must be run first
type HydrateDependencies struct {
	Func    HydrateFunc
	Depends []HydrateFunc
}

type HydrateCall struct {
	Func HydrateFunc
	// the dependencies expressed using function name
	Depends []string
}

func newHydrateCall(hydrateFunc HydrateFunc, dependencies []HydrateFunc) *HydrateCall {
	res := &HydrateCall{Func: hydrateFunc}
	for _, f := range dependencies {
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
	return con.StartIfAllowed(name)
}

func (h *HydrateCall) Start(ctx context.Context, r *RowData, hydrateFuncName string) {
	r.wg.Add(1)

	// call callHydrate async, ignoring return values
	go func() {
		r.callHydrate(ctx, r.queryData, h.Func, hydrateFuncName)
		// decrement number of hydrate functions running
		con.Finished(hydrateFuncName)
	}()
}

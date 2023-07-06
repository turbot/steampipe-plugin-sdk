package plugin

import (
	"context"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
	"log"
	"sync/atomic"
	"time"
)

// hydrateCall struct encapsulates a hydrate call, its config and dependencies
type hydrateCall struct {
	Func HydrateFunc
	// the dependencies expressed using function name
	Depends []string
	Config  *HydrateConfig
	Name    string
}

func newHydrateCall(config *HydrateConfig) *hydrateCall {
	res := &hydrateCall{
		Name:   helpers.GetFunctionName(config.Func),
		Func:   config.Func,
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
func (h hydrateCall) canStart(rowData *rowData, name string, concurrencyManager *concurrencyManager) bool {
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
func (h *hydrateCall) start(ctx context.Context, r *rowData, d *QueryData, concurrencyManager *concurrencyManager) {
	t := time.Now()
	log.Printf("[INFO] start hydrate call %s, wait for rate limiter (%s)", h.Name, d.connectionCallId)
	// get the rate limiter
	rateLimiter := d.plugin.rateLimiters

	var rateLimiterKeys = rate_limiter.KeyMap{
		rate_limiter.RateLimiterKeyHydrate:    h.Name,
		rate_limiter.RateLimiterKeyConnection: d.Connection.Name,
		// TODO add matrix quals if needed
	}
	// wait until we can execute
	rateLimiter.Wait(ctx, rateLimiterKeys)

	log.Printf("[INFO] ****** AFTER rate limiter %s (%dms) (%s)", h.Name, time.Since(t).Milliseconds(), d.connectionCallId)

	// tell the rowdata to wait for this call to complete
	r.wg.Add(1)
	// update the hydrate count
	atomic.AddInt64(&d.queryStatus.hydrateCalls, 1)

	// call callHydrate async, ignoring return values
	go func() {
		r.callHydrate(ctx, d, h.Func, h.Name, h.Config)
		// decrement number of hydrate functions running
		concurrencyManager.Finished(h.Name)
	}()
}

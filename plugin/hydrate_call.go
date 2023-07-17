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

	queryData   *QueryData
	rateLimiter *rate_limiter.MultiLimiter
}

func newHydrateCall(config *HydrateConfig, d *QueryData) (*hydrateCall, error) {
	res := &hydrateCall{
		Name:      helpers.GetFunctionName(config.Func),
		Func:      config.Func,
		Config:    config,
		queryData: d,
	}

	if err := res.initialiseRateLimiter(); err != nil {
		return nil, err
	}

	for _, f := range config.Depends {
		res.Depends = append(res.Depends, helpers.GetFunctionName(f))
	}
	return res, nil
}

// identify any rate limiters which apply to this hydrate call
func (h *hydrateCall) initialiseRateLimiter() error {
	log.Printf("[INFO] hydrateCall %s initialiseRateLimiter (%s)", h.Name, h.queryData.connectionCallId)

	// ask plugin to build a rate limiter for us
	p := h.queryData.plugin

	// now try to construct a multi rate limiter for this call
	rateLimiter, err := p.getHydrateCallRateLimiter(h.Config.RateLimit.Definitions, h.Config.RateLimit.StaticScopeValues, h.queryData)
	if err != nil {
		log.Printf("[WARN] hydrateCall %s getHydrateCallRateLimiter failed: %s (%s)", h.Name, err.Error(), h.queryData.connectionCallId)
		return err
	}

	h.rateLimiter = rateLimiter

	return nil
}

// CanStart returns whether this hydrate call can execute
// - check whether all dependency hydrate functions have been completed
// - check whether the concurrency limits would be exceeded
func (h *hydrateCall) canStart(rowData *rowData, name string, concurrencyManager *concurrencyManager) bool {
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
	return concurrencyManager.StartIfAllowed(name, h.Config.RateLimit.MaxConcurrency)
}

// Start starts a hydrate call
func (h *hydrateCall) start(ctx context.Context, r *rowData, d *QueryData, concurrencyManager *concurrencyManager) {
	h.rateLimit(ctx, d)

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

func (h *hydrateCall) rateLimit(ctx context.Context, d *QueryData) {
	if !rate_limiter.RateLimiterEnabled() {
		log.Printf("[TRACE] start hydrate call, rate limiting disabled %s (%s)", h.Name, d.connectionCallId)
		return
	}
	t := time.Now()

	log.Printf("[INFO] ****** start hydrate call %s, wait for rate limiter (%s)", h.Name, d.connectionCallId)

	// wait until we can execute
	h.rateLimiter.Wait(ctx, h.Config.RateLimit.Cost)

	log.Printf("[INFO] ****** AFTER rate limiter %s (%dms) (%s)", h.Name, time.Since(t).Milliseconds(), d.connectionCallId)
}

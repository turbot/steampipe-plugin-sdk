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
	NamedHydrateFunc
	// the dependencies expressed using function name
	Depends []NamedHydrateFunc
	Config  *HydrateConfig

	queryData   *QueryData
	rateLimiter *rate_limiter.MultiLimiter
}

func newHydrateCall(config *HydrateConfig, d *QueryData) (*hydrateCall, error) {
	res := &hydrateCall{
		Config:    config,
		queryData: d,
		// default to empty limiter
		rateLimiter: rate_limiter.EmptyMultiLimiter(),
	}
	res.NamedHydrateFunc = config.namedHydrate

	for _, f := range config.Depends {
		res.Depends = append(res.Depends, newNamedHydrateFunc(f))
	}

	return res, nil
}

func (h *hydrateCall) shallowCopy() *hydrateCall {
	return &hydrateCall{
		NamedHydrateFunc: NamedHydrateFunc{
			Func: h.Func,
			Name: h.Name,
		},
		Depends:     h.Depends,
		Config:      h.Config,
		queryData:   h.queryData,
		rateLimiter: h.rateLimiter,
	}
}

// identify any rate limiters which apply to this hydrate call
func (h *hydrateCall) initialiseRateLimiter() error {
	log.Printf("[INFO] hydrateCall %s initialiseRateLimiter (%s)", h.Name, h.queryData.connectionCallId)

	// if this call is memoized, do not assign a rate limiter
	if h.IsMemoized {
		log.Printf("[INFO] hydrateCall %s is memoized - assign an empty rate limiter (%s)", h.Name, h.queryData.connectionCallId)
		return nil
	}
	// ask plugin to build a rate limiter for us
	p := h.queryData.plugin

	// now try to construct a multi rate limiter for this call
	rateLimiter, err := p.getHydrateCallRateLimiter(h.Config.Tags, h.queryData)
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
func (h *hydrateCall) canStart(rowData *rowData) bool {
	// check whether all hydrate functions we depend on have saved their results
	for _, dep := range h.Depends {
		if !helpers.StringSliceContains(rowData.getHydrateKeys(), dep.Name) {
			return false
		}
	}
	// if no rate limiting config is defined, we cna start
	if h.rateLimiter == nil {
		return true
	}
	// so a rate limiting config is defined - check whether we satisfy the concurrency limits
	canStart := h.rateLimiter.TryToAcquireSemaphore()

	// record the concurrency delay for this call
	rowData.setHydrateCanStart(h.Name, canStart)

	return canStart
}

// Start starts a hydrate call
func (h *hydrateCall) start(ctx context.Context, r *rowData, d *QueryData) time.Duration {
	var rateLimitDelay time.Duration
	// if we are memoized there is no need to rate limit
	if !h.IsMemoized {
		rateLimitDelay = h.rateLimit(ctx, d)
	}

	// tell the rowData to wait for this call to complete
	r.wg.Add(1)
	// update the hydrate count
	atomic.AddInt64(&d.queryStatus.hydrateCalls, 1)

	// call callHydrate async, ignoring return values
	go func() {
		r.callHydrate(ctx, d, h.NamedHydrateFunc, h.Config)
		h.onFinished()
	}()
	// retrieve the concurrencyDelay for the call
	concurrencyDelay := r.getHydrateConcurrencyDelay(h.Name)
	return rateLimitDelay + concurrencyDelay
}

func (h *hydrateCall) rateLimit(ctx context.Context, d *QueryData) time.Duration {
	// not expected as if there ar eno rate limiters we should have an empty MultiLimiter
	if h.rateLimiter == nil {
		log.Printf("[WARN] hydrate call %s has a nil rateLimiter - not expected", h.Name)
		return 0
	}
	log.Printf("[TRACE] ****** start hydrate call %s, wait for rate limiter (%s)", h.Name, d.connectionCallId)

	// wait until we can execute
	delay := h.rateLimiter.Wait(ctx)

	log.Printf("[TRACE] ****** AFTER rate limiter %s (%dms) (%s)", h.Name, delay.Milliseconds(), d.connectionCallId)

	return delay
}

func (h *hydrateCall) onFinished() {
	if h.rateLimiter != nil {
		h.rateLimiter.ReleaseSemaphore()
	}
}

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
	namedHydrateFunc
	// the dependencies expressed using function name
	Depends []string
	Config  *HydrateConfig

	queryData   *QueryData
	rateLimiter *rate_limiter.MultiLimiter
	// the time when we _could_ start the call, if comncurrency limits allowed
	potentialStartTime time.Time
	concurrencyDelay   time.Duration
}

func newHydrateCall(config *HydrateConfig, d *QueryData) (*hydrateCall, error) {
	res := &hydrateCall{
		Config:    config,
		queryData: d,
	}
	res.namedHydrateFunc = newNamedHydrateFunc(config.Func)

	for _, f := range config.Depends {
		res.Depends = append(res.Depends, helpers.GetFunctionName(f))
	}

	return res, nil
}

func (h *hydrateCall) shallowCopy() *hydrateCall {
	return &hydrateCall{
		namedHydrateFunc: namedHydrateFunc{
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
		if !helpers.StringSliceContains(rowData.getHydrateKeys(), dep) {
			return false
		}
	}
	// so all dependencies have been satisfied
	// if this is a memoized function, no rate limiting is required - we can start
	if h.IsMemoized {
		return true
	}
	// if no rate limiting config is defined, we cna start
	if h.rateLimiter == nil {
		return true
	}
	// so a rate limiting config is defined - check whether we satisfy the concurrency limits

	// if no potential start time is set, set it now
	if h.potentialStartTime.IsZero() {
		h.potentialStartTime = time.Now()
	}

	canStart := h.rateLimiter.TryToAcquireSemaphore()
	if canStart {
		// record the delay in startiung due to concurrency limits
		h.concurrencyDelay = time.Since(h.potentialStartTime)
	}
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
		r.callHydrate(ctx, d, h.namedHydrateFunc, h.Config)
		h.onFinished()
	}()
	return rateLimitDelay + h.concurrencyDelay
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

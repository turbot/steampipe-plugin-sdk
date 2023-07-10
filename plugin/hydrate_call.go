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
	Depends           []string
	Config            *HydrateConfig
	Name              string
	RateLimiterConfig *rate_limiter.Config
	RateLimiterTags   map[string]string
	queryData         *QueryData
}

func newHydrateCall(config *HydrateConfig, d *QueryData) *hydrateCall {
	res := &hydrateCall{
		Name:      helpers.GetFunctionName(config.Func),
		Func:      config.Func,
		Config:    config,
		queryData: d,
	}
	res.setRateLimiterProperties()
	for _, f := range config.Depends {
		res.Depends = append(res.Depends, helpers.GetFunctionName(f))
	}
	return res
}

// resolve the rate limiter config and the tags values which apply
func (h *hydrateCall) setRateLimiterProperties() {
	log.Printf("[INFO] hydrateCall %s setRateLimiterProperties (%s)", h.Name, h.queryData.connectionCallId)

	// first resolve the rate limiter config by merging the tree of possible configs
	h.RateLimiterConfig = h.resolveRateLimiterConfig()

	// now build the set of all tag values which applies to this call
	h.RateLimiterTags = h.getRateLimiterTagValues()
}

func (h *hydrateCall) resolveRateLimiterConfig() *rate_limiter.Config {
	log.Printf("[INFO] hydrateCall %s resolveRateLimiterConfig (%s)", h.Name, h.queryData.connectionCallId)

	// build an array of configs to combine, in order of precedence
	var configs []*rate_limiter.Config
	hydrateRateLimiterConfig := h.Config.RateLimiterConfig
	pluginDefaultRateLimiterConfig := h.queryData.plugin.DefaultRateLimiterConfig

	// rate limiter config in the hydrate config takes highest precedence
	if hydrateRateLimiterConfig != nil {
		configs = append(configs, hydrateRateLimiterConfig)
		log.Printf("[INFO] hydrate rate limiter config: %s (%s)", hydrateRateLimiterConfig, h.queryData.connectionCallId)
	}
	// then the plugin default rate limiter config
	if pluginDefaultRateLimiterConfig != nil {
		configs = append(configs, pluginDefaultRateLimiterConfig)
		log.Printf("[INFO] plugin default rate limiter config: %s (%s)", pluginDefaultRateLimiterConfig, h.queryData.connectionCallId)
	}

	// then the base default
	configs = append(configs, rate_limiter.DefaultConfig())
	log.Printf("[INFO]  default rate limiter config: %s (%s)", rate_limiter.DefaultConfig(), h.queryData.connectionCallId)

	res := rate_limiter.CombineConfigs(configs)

	log.Printf("[INFO] hydrateCall %s resolved rate limiter config: %s (%s)", h.Name, res, h.queryData.connectionCallId)

	return res
}

func (h *hydrateCall) getRateLimiterTagValues() map[string]string {
	log.Printf("[INFO] hydrateCall %s getRateLimiterTagValues (%s)", h.Name, h.queryData.connectionCallId)

	callSpecificTags := h.Config.Tags
	baseTags := h.queryData.rateLimiterTags

	log.Printf("[INFO] callSpecificTags: %s (%s)", formatStringMap(callSpecificTags), h.queryData.connectionCallId)
	log.Printf("[INFO] baseTags: %s (%s)", formatStringMap(baseTags), h.queryData.connectionCallId)

	// if there are no call-specific tags, just used the base tags defined in query data
	if len(callSpecificTags) == 0 {
		return baseTags
	}
	// otherwise, if we need to include call-specific tags, clone the base tags
	allTagValues := make(map[string]string, len(baseTags)+len(callSpecificTags))
	// copy base tags from query data
	for k, v := range baseTags {
		allTagValues[k] = v
	}
	// add/replace with call specific tags
	for k, v := range callSpecificTags {
		allTagValues[k] = v
	}

	// now filter this down to the tags which the rate limiter config needs
	res := make(map[string]string, len(h.RateLimiterConfig.Tags))
	for _, tag := range h.RateLimiterConfig.Tags {
		val, ok := allTagValues[tag]
		if !ok {
			// if no value is set, val will defauly to empty string - just use this as the value
			log.Printf("[INFO] setRateLimiterProperties for hydrate call %s - no value found for tag %s (%s)", h.Name, tag, h.queryData.connectionCallId)
		}
		res[tag] = val
	}

	return res
}

func formatStringMap(tags map[string]string) string {
	var strs []string
	for k, v := range tags {
		strs = append()
	}

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

func (h *hydrateCall) rateLimit(ctx context.Context, d *QueryData) error {
	if !rate_limiter.RateLimiterEnabled() {
		log.Printf("[TRACE] start hydrate call, rate limiting disabled %s (%s)", h.Name, d.connectionCallId)
		return nil
	}
	t := time.Now()

	log.Printf("[INFO] start hydrate call %s, wait for rate limiter (%s)", h.Name, d.connectionCallId)
	// get the rate limiter
	rateLimiter, err := d.plugin.rateLimiters.GetOrCreate(h.RateLimiterTags, h.RateLimiterConfig)
	if err != nil {
		return err
	}

	// wait until we can execute
	// NOTE - we wait once for each unit of cost
	// TODO CHECK THIS
	for i := 0; i < h.Config.Cost; i++ {
		rateLimiter.Wait(ctx)
	}

	log.Printf("[INFO] ****** AFTER rate limiter %s (%dms) (%s)", h.Name, time.Since(t).Milliseconds(), d.connectionCallId)
	return nil
}

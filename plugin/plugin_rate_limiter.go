package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
	"log"
)

func (p *Plugin) getHydrateCallRateLimiter(hydrateCallRateLimitConfig *rate_limiter.Config, hydrateCallRateLimiterTagValues map[string]string, queryData *QueryData) (*rate_limiter.MultiLimiter, error) {
	log.Printf("[INFO] getHydrateCallRateLimiter")

	// first resolve the rate limiter config by building the list of rate limiter definitions from the various sources
	// - plugin config (coming soon)
	// - hydrate config rate limiter defs
	// - plugin level rate limiter defs
	// - default rate limiter
	resolvedRateLimiterConfig := p.resolveRateLimiterConfig(hydrateCallRateLimitConfig)

	log.Printf("[INFO] resolvedRateLimiterConfig: %s", resolvedRateLimiterConfig)

	// now build the set of all tag values which applies to this call
	rateLimiterTagValues := queryData.getRateLimiterTagValues(hydrateCallRateLimiterTagValues)

	log.Printf("[INFO] rateLimiterTagValues: %s", rate_limiter.FormatStringMap(rateLimiterTagValues))

	// build a list of all the limiters which match these tags
	limiters, err := p.getRateLimitersForTagValues(resolvedRateLimiterConfig, rateLimiterTagValues)
	if err != nil {
		return nil, err
	}

	res := &rate_limiter.MultiLimiter{
		Limiters: limiters,
	}

	log.Printf("[INFO] returning multi limiter: %s", res)

	return res, nil
}

func (p *Plugin) getRateLimitersForTagValues(resolvedRateLimiterConfig *rate_limiter.Config, rateLimiterTagValues map[string]string) ([]*rate_limiter.Limiter, error) {
	var limiters []*rate_limiter.Limiter
	for _, l := range resolvedRateLimiterConfig.Limiters {
		// build a filtered map of just the tag values required fopr this limiter
		// if we DO NOT have values for all required tags, requiredTagValues will be nil
		// and this limiter DOES NOT APPLY TO US
		requiredTagValues := l.GetRequiredTagValues(rateLimiterTagValues)
		if requiredTagValues != nil {
			// this limiter does apply to us, get or create a limiter instance
			limiter, err := p.rateLimiters.GetOrCreate(l, requiredTagValues)
			if err != nil {
				return nil, err
			}
			limiters = append(limiters, limiter)
		}
	}
	return limiters, nil
}

func (p *Plugin) resolveRateLimiterConfig(hydrateCallRateLimitConfigs *rate_limiter.Config) *rate_limiter.Config {
	// build list of source limiter configs we will merge
	sourceConfigs := []*rate_limiter.Config{
		hydrateCallRateLimitConfigs,
		p.RateLimiterConfig,
		rate_limiter.DefaultConfig(),
	}
	// build an array of rate limiter configs to combine, in order of precedence
	var res = &rate_limiter.Config{}
	for _, c := range sourceConfigs {
		res.Merge(c)
		if res.FinalLimiter {
			return res
		}
	}

	return res
}

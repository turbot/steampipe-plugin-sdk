package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
	"log"
)

func (p *Plugin) getHydrateCallRateLimiter(hydrateCallDefs *rate_limiter.Definitions, hydrateCallStaticScopeValues map[string]string, queryData *QueryData) (*rate_limiter.MultiLimiter, error) {
	log.Printf("[INFO] getHydrateCallRateLimiter")

	res := &rate_limiter.MultiLimiter{}
	// first resolve the rate limiter definitions by building the list of rate limiter definitions from the various sources
	// - plugin config file (coming soon)
	// - hydrate config rate limiter defs
	// - plugin level rate limiter defs
	// - default rate limiter
	rateLimiterDefs := p.resolveRateLimiterConfig(hydrateCallDefs, queryData.Table.RateLimit)
	// short circuit if there ar eno defs
	if len(rateLimiterDefs.Limiters) == 0 {
		log.Printf("[INFO] resolvedRateLimiterConfig: no rate limiters (%s)", queryData.connectionCallId)
		return res, nil
	}

	log.Printf("[INFO] resolvedRateLimiterConfig: %s (%s)", rateLimiterDefs, queryData.connectionCallId)

	// wrape static scope values in a ScopeValues struct
	hydrateCallScopeValues := rate_limiter.NewRateLimiterScopeValues()
	hydrateCallScopeValues.StaticValues = hydrateCallStaticScopeValues

	// now build the set of all tag values which applies to this call
	rateLimiterScopeValues := queryData.resolveRateLimiterScopeValues(hydrateCallScopeValues)

	log.Printf("[INFO] rateLimiterTagValues: %s", rateLimiterScopeValues)

	// build a list of all the limiters which match these tags
	limiters, err := p.getRateLimitersForScopeValues(rateLimiterDefs, rateLimiterScopeValues)
	if err != nil {
		return nil, err
	}

	// finally package them into a multi-limiter
	res.Limiters = limiters

	log.Printf("[INFO] returning multi limiter: %s", res)

	return res, nil
}

func (p *Plugin) getRateLimitersForScopeValues(defs *rate_limiter.Definitions, scopeValues *rate_limiter.ScopeValues) ([]*rate_limiter.Limiter, error) {
	var limiters []*rate_limiter.Limiter

	for _, l := range defs.Limiters {
		// build a filtered map of just the scope values required for this limiter
		requiredScopeValues, gotAllValues := l.Scopes.GetRequiredValues(scopeValues)
		// do we have all the required values?
		if !gotAllValues {
			// this rate limiter does not apply
			continue
		}

		// now check whether the tag valkues satisfy any filters the limiter definition has
		if !l.SatisfiesFilters(requiredScopeValues) {
			continue
		}

		// this limiter DOES apply to us, get or create a limiter instance
		limiter, err := p.rateLimiters.GetOrCreate(l, requiredScopeValues)
		if err != nil {
			return nil, err
		}
		limiters = append(limiters, limiter)
	}
	return limiters, nil
}

func (p *Plugin) resolveRateLimiterConfig(hydrateCallDefs, tableDefs *rate_limiter.Definitions) *rate_limiter.Definitions {
	// build list of source limiter configs we will merge
	sourceConfigs := []*rate_limiter.Definitions{
		hydrateCallDefs,
		tableDefs,
		p.RateLimiters,
		rate_limiter.DefaultConfig(),
	}
	// build an array of rate limiter configs to combine, in order of precedence
	var res = &rate_limiter.Definitions{}
	for _, c := range sourceConfigs {
		res.Merge(c)
		if res.FinalLimiter {
			return res
		}
	}

	return res
}

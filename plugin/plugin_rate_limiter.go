package plugin

import (
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
	"log"
)

func (p *Plugin) getHydrateCallRateLimiter(hydrateCallScopeValues map[string]string, queryData *QueryData) (*rate_limiter.MultiLimiter, error) {
	log.Printf("[INFO] getHydrateCallRateLimiter")

	res := &rate_limiter.MultiLimiter{}
	// short circuit if there ar eno defs
	if len(p.resolvedRateLimiterDefs) == 0 {
		log.Printf("[INFO] resolvedRateLimiterConfig: no rate limiters (%s)", queryData.connectionCallId)
		return res, nil
	}

	// now build the set of all tag values which applies to this call
	rateLimiterScopeValues := queryData.resolveRateLimiterScopeValues(hydrateCallScopeValues)

	log.Printf("[INFO] rateLimiterTagValues: %s", rateLimiterScopeValues)

	// build a list of all the limiters which match these tags
	limiters, err := p.getRateLimitersForScopeValues(rateLimiterScopeValues)
	if err != nil {
		return nil, err
	}

	// finally package them into a multi-limiter
	res = rate_limiter.NewMultiLimiter(limiters, rateLimiterScopeValues)

	log.Printf("[INFO] returning multi limiter: %s", res)

	return res, nil
}

func (p *Plugin) getRateLimitersForScopeValues(scopeValues map[string]string) ([]*rate_limiter.Limiter, error) {
	var limiters []*rate_limiter.Limiter
	// lock the map
	p.rateLimiterDefsMut.RLock()
	defer p.rateLimiterDefsMut.RUnlock()

	// NOTE: use rateLimiterLookup NOT the public RateLimiter property.
	// This is to ensure config overrides are respected
	for _, l := range p.resolvedRateLimiterDefs {
		// build a filtered map of just the scope values required for this limiter
		requiredScopeValues := helpers.FilterMap(scopeValues, l.Scope)
		// do we have all the required values?
		if len(requiredScopeValues) < len(l.Scope) {
			// this rate limiter does not apply
			continue
		}

		// now check whether the tag values satisfy any filters the limiter definition has
		if !l.SatisfiesFilters(requiredScopeValues) {
			continue
		}

		// this limiter DOES apply to us, get or create a limiter instance
		limiter, err := p.rateLimiterInstances.GetOrCreate(l, requiredScopeValues)
		if err != nil {
			return nil, err
		}
		limiters = append(limiters, limiter)
	}
	return limiters, nil
}

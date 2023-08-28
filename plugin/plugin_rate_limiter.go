package plugin

import (
	"github.com/gertd/go-pluralize"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
	"golang.org/x/exp/maps"
	"log"
	"strings"
)

func (p *Plugin) getHydrateCallRateLimiter(hydrateCallScopeValues map[string]string, queryData *QueryData) (*rate_limiter.MultiLimiter, error) {
	log.Printf("[INFO] getHydrateCallRateLimiter (%s)", queryData.connectionCallId)

	res := &rate_limiter.MultiLimiter{}
	// short circuit if there ar eno defs
	if len(p.resolvedRateLimiterDefs) == 0 {
		log.Printf("[INFO] resolvedRateLimiterConfig: no rate limiters (%s)", queryData.connectionCallId)
		return res, nil
	}

	// now build the set of all tag values which applies to this call
	rateLimiterScopeValues := queryData.resolveRateLimiterScopeValues(hydrateCallScopeValues)

	log.Printf("[INFO] rateLimiterScopeValues: %s", rateLimiterScopeValues)

	// build a list of all the limiters which match these tags
	limiters, err := p.getRateLimitersForScopeValues(rateLimiterScopeValues)
	if err != nil {
		return nil, err
	}

	log.Printf("[INFO] found %d matching %s",
		len(limiters),
		pluralize.NewClient().Pluralize("limiter", len(limiters), false))

	// finally package them into a multi-limiter
	res = rate_limiter.NewMultiLimiter(limiters, rateLimiterScopeValues)

	log.Printf("[INFO] returning multi limiter: %s", res)

	return res, nil
}

func (p *Plugin) getRateLimitersForScopeValues(scopeValues map[string]string) ([]*rate_limiter.HydrateLimiter, error) {
	h := helpers.GetMD5Hash(rate_limiter.FormatStringMap(scopeValues))
	h = h[len(h)-4:]
	log.Printf("[INFO] getRateLimitersForScopeValues (%s)", h)
	log.Printf("[INFO] scope values: %v (%s)", scopeValues, h)
	log.Printf("[INFO] resolvedRateLimiterDefs: %s (%s)", strings.Join(maps.Keys(p.resolvedRateLimiterDefs), ","), h)

	// put limiters in map to dedupe
	var limiters = make(map[string]*rate_limiter.HydrateLimiter)
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
			log.Printf("[INFO] we DO NOT have scope values required by limiter '%s' - it requires: %s (%s)", l.Name, strings.Join(l.Scope, ","), h)
			// this rate limiter does not apply
			continue
		}

		// now check whether the tag values satisfy any filters the limiter definition has
		if !l.SatisfiesFilters(requiredScopeValues) {
			log.Printf("[INFO] we DO NOT satisfy the filter for limiter '%s' - filter: %s (%s)", l.Name, l.Where, h)
			continue
		}

		// this limiter DOES apply to us, get or create a limiter instance
		log.Printf("[INFO] limiter '%s' DOES apply to us (%s)", l.Name, h)

		limiter, err := p.rateLimiterInstances.GetOrCreate(l, requiredScopeValues)
		if err != nil {
			return nil, err
		}
		// this limiter DOES apply to us, get or create a limiter instance
		log.Printf("[INFO] got limiter instance for '%s'(%s)", limiter.Name, h)

		limiters[limiter.Name] = limiter
	}
	return maps.Values(limiters), nil
}

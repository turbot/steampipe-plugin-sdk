package plugin

import "github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"

func (d *QueryData) resolveRateLimiterScopeValues(hydrateCallScopeValues *rate_limiter.ScopeValues) *rate_limiter.ScopeValues {
	// make a new map to populate
	res := rate_limiter.NewRateLimiterScopeValues()

	// build list of source value maps which we will merge
	// this is in order of DECREASING precedence, i.e. highest first
	scopeValueList := []*rate_limiter.ScopeValues{
		// static tag values defined by hydrate config
		hydrateCallScopeValues,
		// tag values for this scan (mix of statix and colum tag values)
		d.rateLimiterScopeValues,
	}

	for _, scopeValues := range scopeValueList {
		// add any scope values which are not already set
		res.Merge(scopeValues)
	}
	return res
}

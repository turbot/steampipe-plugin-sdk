package plugin

import (
	"context"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/quals"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
	"log"
)

func (d *QueryData) WaitForListRateLimit(ctx context.Context) {
	if d.Table.List.ParentHydrate != nil {
		d.fetchLimiters.childListWait(ctx)
	} else {
		d.fetchLimiters.wait(ctx)
	}
}

// resolve the scope values for a given hydrate call
func (d *QueryData) resolveRateLimiterScopeValues(hydrateCallScopeValues *rate_limiter.ScopeValues) *rate_limiter.ScopeValues {
	// make a new map to populate
	res := rate_limiter.NewRateLimiterScopeValues()

	tableScopeValues := rate_limiter.NewRateLimiterScopeValues()
	tableScopeValues.StaticValues = d.Table.RateLimit.ScopeValues

	// build list of source value maps which we will merge
	// this is in order of DECREASING precedence, i.e. highest first
	scopeValueList := []*rate_limiter.ScopeValues{
		// static scope values defined by hydrate config
		hydrateCallScopeValues,
		// static scope values defined by table config
		tableScopeValues,
		// scope values for this scan (static and column values)
		d.rateLimiterScopeValues,
	}

	for _, scopeValues := range scopeValueList {
		// add any scope values which are not already set
		res.Merge(scopeValues)
	}
	return res
}

/*
	build the base set of scope used to resolve a rate limiter

this will consist of:
- plugin, connection and table name
- quals (with value as string)
*/
func (d *QueryData) populateRateLimitScopeValues() {
	d.rateLimiterScopeValues = rate_limiter.NewRateLimiterScopeValues()

	// add the connection to static scope values
	d.rateLimiterScopeValues.StaticValues[rate_limiter.RateLimiterScopeConnection] = d.Connection.Name

	// dynamic scope values (qual values)
	for column, qualsForColumn := range d.Quals {
		for _, qual := range qualsForColumn.Quals {
			if qual.Operator == quals.QualOperatorEqual {
				qualValueString := grpc.GetQualValueString(qual.Value)
				d.rateLimiterScopeValues.ColumnValues[column] = qualValueString
			}
		}
	}
}

func (d *QueryData) resolveFetchRateLimiters() error {
	d.fetchLimiters = &fetchCallRateLimiters{}
	// is it a get
	if d.FetchType == fetchTypeGet {
		return d.resolveGetRateLimiters()
	}

	// otherwise this is a list

	// is there a parent-child hydrate?
	if d.Table.List.ParentHydrate != nil {
		// it is a parent child list
		return d.resolveParentChildRateLimiters()
	}

	// ok it's just a single level list hydrate
	return d.resolveListRateLimiters()
}

func (d *QueryData) resolveGetRateLimiters() error {
	rateLimiterConfig := d.Table.Get.RateLimit
	// NOTE: RateLimit cannot be nil as it is initialized to an empty struct if needed
	getLimiter, err := d.plugin.getHydrateCallRateLimiter(rateLimiterConfig.Definitions, rateLimiterConfig.ScopeValues, d)
	if err != nil {
		log.Printf("[WARN] get call %s getHydrateCallRateLimiter failed: %s (%s)", helpers.GetFunctionName(d.Table.Get.Hydrate), err.Error(), d.connectionCallId)
		return err
	}

	d.fetchLimiters.rateLimiter = getLimiter
	d.fetchLimiters.cost = rateLimiterConfig.Cost

	return nil
}

func (d *QueryData) resolveParentChildRateLimiters() error {
	parentRateLimitConfig := d.Table.List.ParentRateLimit
	childRateLimitConfig := d.Table.List.RateLimit
	// NOTE: RateLimit and ParentRateLimit cannot be nil as they are initialized to an empty struct if needed

	// resolve the parent hydrate rate limiter
	parentRateLimiter, err := d.plugin.getHydrateCallRateLimiter(parentRateLimitConfig.Definitions, parentRateLimitConfig.ScopeValues, d)
	if err != nil {
		log.Printf("[WARN] resolveParentChildRateLimiters: %s: getHydrateCallRateLimiter failed: %s (%s)", helpers.GetFunctionName(d.Table.List.ParentHydrate), err.Error(), d.connectionCallId)
		return err
	}
	// assign the parent rate limiter to d.fetchLimiters
	d.fetchLimiters.rateLimiter = parentRateLimiter
	d.fetchLimiters.cost = parentRateLimitConfig.Cost

	// resolve the child  hydrate rate limiter
	childRateLimiter, err := d.plugin.getHydrateCallRateLimiter(childRateLimitConfig.Definitions, childRateLimitConfig.ScopeValues, d)
	if err != nil {
		log.Printf("[WARN] resolveParentChildRateLimiters: %s: getHydrateCallRateLimiter failed: %s (%s)", helpers.GetFunctionName(d.Table.List.Hydrate), err.Error(), d.connectionCallId)
		return err
	}
	d.fetchLimiters.childListRateLimiter = childRateLimiter
	d.fetchLimiters.childListCost = childRateLimitConfig.Cost

	return nil
}

func (d *QueryData) resolveListRateLimiters() error {
	rateLimiterConfig := d.Table.List.RateLimit
	// NOTE: RateLimit cannot be nil as it is initialized to an empty struct if needed
	listLimiter, err := d.plugin.getHydrateCallRateLimiter(rateLimiterConfig.Definitions, rateLimiterConfig.ScopeValues, d)
	if err != nil {
		log.Printf("[WARN] get call %s getHydrateCallRateLimiter failed: %s (%s)", helpers.GetFunctionName(d.Table.Get.Hydrate), err.Error(), d.connectionCallId)
		return err
	}
	d.fetchLimiters.rateLimiter = listLimiter
	d.fetchLimiters.cost = rateLimiterConfig.Cost
	return nil
}

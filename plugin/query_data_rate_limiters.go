package plugin

import (
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/quals"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
	"log"
)

/*
	build the base set of tag values used to resolve a rate limiter

this will  consist of:
-connection name
- quals (with value as string)
*/
func (d *QueryData) populateRateLimitTags() {
	d.rateLimiterTagValues = make(map[string]string)

	// static tags
	// add the connection
	d.rateLimiterTagValues[rate_limiter.RateLimiterKeyConnection] = d.Connection.Name
	// add plugin
	d.rateLimiterTagValues[rate_limiter.RateLimiterKeyPlugin] = d.plugin.Name
	// add table
	d.rateLimiterTagValues[rate_limiter.RateLimiterKeyTable] = d.Table.Name

	// add the equals quals
	for column, qualsForColumn := range d.Quals {
		for _, qual := range qualsForColumn.Quals {
			if qual.Operator == quals.QualOperatorEqual {
				qualValueString := grpc.GetQualValueString(qual.Value)
				d.rateLimiterTagValues[column] = qualValueString
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
	if d.Table.List.ParentHydrate == nil {
		// ok it's just a single level hydrate
		return d.resolveListRateLimiters()
	}

	// it is a parent child list
	return d.resolveParentChildRateLimiters()
}

func (d *QueryData) resolveGetRateLimiters() error {
	rateLimiterConfig := d.Table.Get.RateLimit
	getLimiter, err := d.plugin.getHydrateCallRateLimiter(rateLimiterConfig.Definitions, rateLimiterConfig.TagValues, d)
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

	parentRateLimiter, err := d.plugin.getHydrateCallRateLimiter(parentRateLimitConfig.Definitions, parentRateLimitConfig.TagValues, d)
	if err != nil {
		log.Printf("[WARN] get call %s getHydrateCallRateLimiter failed: %s (%s)", helpers.GetFunctionName(d.Table.Get.Hydrate), err.Error(), d.connectionCallId)
		return err
	}
	d.fetchLimiters.rateLimiter = parentRateLimiter
	d.fetchLimiters.cost = parentRateLimitConfig.Cost

	childRateLimiter, err := d.plugin.getHydrateCallRateLimiter(childRateLimitConfig.Definitions, childRateLimitConfig.TagValues, d)
	if err != nil {
		log.Printf("[WARN] get call %s getHydrateCallRateLimiter failed: %s (%s)", helpers.GetFunctionName(d.Table.Get.Hydrate), err.Error(), d.connectionCallId)
		return err
	}
	d.fetchLimiters.childListRateLimiter = childRateLimiter
	d.fetchLimiters.childListCost = childRateLimitConfig.Cost

	return nil
}

func (d *QueryData) resolveListRateLimiters() error {
	rateLimiterConfig := d.Table.List.RateLimit
	listLimiter, err := d.plugin.getHydrateCallRateLimiter(rateLimiterConfig.Definitions, rateLimiterConfig.TagValues, d)
	if err != nil {
		log.Printf("[WARN] get call %s getHydrateCallRateLimiter failed: %s (%s)", helpers.GetFunctionName(d.Table.Get.Hydrate), err.Error(), d.connectionCallId)
		return err
	}
	d.fetchLimiters.rateLimiter = listLimiter
	d.fetchLimiters.cost = rateLimiterConfig.Cost
	return nil
}

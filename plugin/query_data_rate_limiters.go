package plugin

import (
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/quals"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
	"log"
	"time"
)

func (d *QueryData) initialiseRateLimiters() {
	log.Printf("[INFO] initialiseRateLimiters for query data %p (%s)", d, d.connectionCallId)
	// build the base set of scope values used to resolve a rate limiter
	d.populateRateLimitScopeValues()

	// populate the rate limiters for the fetch call(s) (get/list/parent-list)
	d.resolveFetchRateLimiters()

	// populate the rate limiters for the hydrate calls
	d.resolveHydrateRateLimiters()
}

// resolve the scope values for a given hydrate call
func (d *QueryData) resolveRateLimiterScopeValues(hydrateCallScopeValues map[string]string) map[string]string {
	log.Printf("[INFO] resolveRateLimiterScopeValues (%s)", d.connectionCallId)
	log.Printf("[INFO] hydrateCallScopeValues %v", hydrateCallScopeValues)
	log.Printf("[INFO] d.Table.Tags %v", d.Table.Tags)
	log.Printf("[INFO] d.rateLimiterScopeValues %v", d.rateLimiterScopeValues)
	// build list of source value maps which we will merge
	// this is in order of DECREASING precedence, i.e. highest first
	scopeValueList := []map[string]string{
		// static scope values defined by hydrate config
		hydrateCallScopeValues,
		// static scope values defined by table config
		d.Table.Tags,
		// scope values for this scan (static and column values)
		d.rateLimiterScopeValues,
	}

	// merge these in precedence order
	res := rate_limiter.MergeScopeValues(scopeValueList)
	log.Printf("[INFO] merged scope values %v", res)
	return res
}

/*
	build the base set of scope used to resolve a rate limiter

this will consist of:
- plugin, connection and table name
- quals (with value as string)
*/
func (d *QueryData) populateRateLimitScopeValues() {
	d.rateLimiterScopeValues = map[string]string{}

	// add the connection
	d.rateLimiterScopeValues[rate_limiter.RateLimiterScopeConnection] = d.Connection.Name
	// add matrix quals


	for column, qualsForColumn := range d.Quals {
		if _, isMatrixQual := d.matrixColLookup[column]; isMatrixQual {
			for _, qual := range qualsForColumn.Quals {
				if qual.Operator == quals.QualOperatorEqual {
					qualValueString := grpc.GetQualValueString(qual.Value)
					d.rateLimiterScopeValues[column] = qualValueString
				}
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
	// NOTE: RateLimit cannot be nil as it is initialized to an empty struct if needed
	getLimiter, err := d.plugin.getHydrateCallRateLimiter(d.Table.Get.Tags, d)
	if err != nil {
		log.Printf("[WARN] get call %s getHydrateCallRateLimiter failed: %s (%s)", helpers.GetFunctionName(d.Table.Get.Hydrate), err.Error(), d.connectionCallId)
		return err
	}

	d.fetchLimiters.rateLimiter = getLimiter
	return nil
}

func (d *QueryData) resolveParentChildRateLimiters() error {

	// NOTE: RateLimit and ParentRateLimit cannot be nil as they are initialized to an empty struct if needed

	// resolve the parent hydrate rate limiter
	parentRateLimiter, err := d.plugin.getHydrateCallRateLimiter(d.Table.List.ParentTags, d)
	if err != nil {
		log.Printf("[WARN] resolveParentChildRateLimiters: %s: getHydrateCallRateLimiter failed: %s (%s)", helpers.GetFunctionName(d.Table.List.ParentHydrate), err.Error(), d.connectionCallId)
		return err
	}
	// assign the parent rate limiter to d.fetchLimiters
	d.fetchLimiters.rateLimiter = parentRateLimiter

	// resolve the child  hydrate rate limiter
	childRateLimiter, err := d.plugin.getHydrateCallRateLimiter(d.Table.List.Tags, d)
	if err != nil {
		log.Printf("[WARN] resolveParentChildRateLimiters: %s: getHydrateCallRateLimiter failed: %s (%s)", helpers.GetFunctionName(d.Table.List.Hydrate), err.Error(), d.connectionCallId)
		return err
	}
	d.fetchLimiters.childListRateLimiter = childRateLimiter

	return nil
}

func (d *QueryData) resolveListRateLimiters() error {
	// NOTE: RateLimit cannot be nil as it is initialized to an empty struct if needed
	listLimiter, err := d.plugin.getHydrateCallRateLimiter(d.Table.List.Tags, d)
	if err != nil {
		log.Printf("[WARN] get call %s getHydrateCallRateLimiter failed: %s (%s)", helpers.GetFunctionName(d.Table.Get.Hydrate), err.Error(), d.connectionCallId)
		return err
	}
	d.fetchLimiters.rateLimiter = listLimiter
	return nil
}

func (d *QueryData) setListLimiterMetadata(fetchDelay time.Duration) {
	fetchMetadata := &hydrateMetadata{
		FuncName:     helpers.GetFunctionName(d.listHydrate),
		RateLimiters: d.fetchLimiters.rateLimiter.LimiterNames(),
		DelayMs:      fetchDelay.Milliseconds(),
	}
	if d.childHydrate == nil {
		fetchMetadata.Type = string(fetchTypeList)
		d.fetchMetadata = fetchMetadata
	} else {
		d.fetchMetadata = &hydrateMetadata{
			Type:         string(fetchTypeList),
			FuncName:     helpers.GetFunctionName(d.childHydrate),
			RateLimiters: d.fetchLimiters.childListRateLimiter.LimiterNames(),
		}
		fetchMetadata.Type = "parentHydrate"
		d.parentHydrateMetadata = fetchMetadata
	}
}

func (d *QueryData) setGetLimiterMetadata(fetchDelay time.Duration) {
	d.fetchMetadata = &hydrateMetadata{
		Type:         string(fetchTypeGet),
		FuncName:     helpers.GetFunctionName(d.Table.Get.Hydrate),
		RateLimiters: d.fetchLimiters.rateLimiter.LimiterNames(),
		DelayMs:      fetchDelay.Milliseconds(),
	}
}

func (d *QueryData) resolveHydrateRateLimiters() error {
	for _, h := range d.hydrateCalls {
		if err := h.initialiseRateLimiter(); err != nil {
			return err
		}
	}
	return nil
}

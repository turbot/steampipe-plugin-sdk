package plugin

import (
	"fmt"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
	"log"
	"strings"

	"github.com/turbot/go-kit/helpers"
)

//
//type RateLimiterConfig struct {
//	Limit rate.Limit
//	Burst int
//}

/*
HydrateConfig defines how to run a [HydrateFunc]:

  - Which errors to ignore: [plugin.HydrateConfig.IgnoreConfig].

  - Which errors to retry: [plugin.HydrateConfig.RetryConfig].

  - How many concurrent calls to allow: [plugin.HydrateConfig.MaxConcurrency].

  - Which hydrate calls must complete before this HydrateFunc can start: [plugin.HydrateConfig.Depends].

It's not valid to have a HydrateConfig for a HydrateFunc that is specified in a [GetConfig].

A HydrateConfig with IgnoreConfig:

	HydrateConfig: []plugin.HydrateConfig{
		{
		Func:           getRetentionPeriod,
		IgnoreConfig:   &plugin.IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreError},
		}

A HydrateConfig with MaxConcurrency:

	HydrateConfig: []plugin.HydrateConfig{
		{
		Func:           getRetentionPeriod,
		MaxConcurrency: 50,
		IgnoreConfig:   &plugin.IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreError},
		}

A HydrateConfig with all fields specified:

	HydrateConfig: []plugin.HydrateConfig{
		{
		Func:           getRetentionPeriod,
		MaxConcurrency: 50,
		IgnoreConfig:   &plugin.IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreError},
		RetryConfig:    &plugin.RetryConfig{
			ShouldRetryErrorFunc: shouldRetryError,
		}

Steampipe parallelizes hydrate functions as much as possible. Sometimes, however, one hydrate function requires the output from another.

	return &plugin.Table{
			Name: "hydrate_columns_dependency",
			List: &plugin.ListConfig{
				Hydrate: hydrateList,
			},
			HydrateConfig: []plugin.HydrateConfig{
				{
					Func:    hydrate2,
					Depends: []plugin.HydrateFunc{hydrate1},
				},
			},
			Columns: []*plugin.Column{
				{Name: "id", Type: proto.ColumnType_INT},
				{Name: "hydrate_column_1", Type: proto.ColumnType_STRING, Hydrate: hydrate1},
				{Name: "hydrate_column_2", Type: proto.ColumnType_STRING, Hydrate: hydrate2},
			},
		}

Here, hydrate function hydrate2 is dependent on hydrate1. This means hydrate2 will not execute until hydrate1 has completed and the results are available. hydrate2 can refer to the results from hydrate1 as follows:

	func hydrate2(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
			// NOTE: in this case we know the output of hydrate1 is map[string]interface{} so we cast it accordingly.
			// the data should be cast to th appropriate type
		hydrate1Results := h.HydrateResults["hydrate1"].(map[string]interface{})
	.....
	}

Note that:
  - Multiple dependencies are supported.
  - Circular dependencies will be detected and cause a validation failure.
  - The Get and List hydrate functions ***CANNOT*** have dependencies.

Examples:
  - [aws]
  - [oci]

[aws]: https://github.com/turbot/steampipe-plugin-aws/blob/f29ae1642edaf51b1189f2e059b27b740a4aa143/aws/table_aws_codeartifact_repository.go#L42-L46
[oci]: https://github.com/turbot/steampipe-plugin-oci/blob/27ddf689f7606009cf26b2716e1634fc91d53585/oci/table_oci_identity_tenancy.go#L23-L27
*/
type HydrateConfig struct {
	Func           HydrateFunc
	MaxConcurrency int
	RetryConfig    *RetryConfig
	IgnoreConfig   *IgnoreConfig
	// deprecated - use IgnoreConfig
	ShouldIgnoreError ErrorPredicate
	Depends           []HydrateFunc

	// tags values used to resolve the rate limiter for this hydrate call
	// for example:
	// "service": "s3"
	//
	// when resolving a rate limiter for a hydrate call, a map of key values is automatically populated from:
	// - the connection name
	// - quals (with vales as string)
	// - tag specified in the hydrate config
	//
	// this map is then used to find a rate limiter
	RateLimiterTagValues map[string]string
	// the hydrate config can define additional rate limiters which apply to this call
	RateLimiterConfig *rate_limiter.Config
	// how expensive is this hydrate call
	// roughly - how many API calls does it hit
	Cost int
}

func (c *HydrateConfig) String() interface{} {
	var dependsStrings = make([]string, len(c.Depends))
	for i, dep := range c.Depends {
		dependsStrings[i] = helpers.GetFunctionName(dep)
	}
	return fmt.Sprintf(`Func: %s
MaxConcurrency: %d
RetryConfig: %s
IgnoreConfig: %s
Depends: %s`,
		helpers.GetFunctionName(c.Func),
		c.MaxConcurrency,
		c.RetryConfig.String(),
		c.IgnoreConfig.String(),
		strings.Join(dependsStrings, ","))
}

func (c *HydrateConfig) initialise(table *Table) {
	log.Printf("[TRACE] HydrateConfig.initialise func %s, table %s", helpers.GetFunctionName(c.Func), table.Name)

	// create RetryConfig if needed
	if c.RetryConfig == nil {
		c.RetryConfig = &RetryConfig{}
	}

	// create DefaultIgnoreConfig if needed
	if c.IgnoreConfig == nil {
		c.IgnoreConfig = &IgnoreConfig{}
	}
	// copy the (deprecated) top level ShouldIgnoreError property into the ignore config
	if c.IgnoreConfig.ShouldIgnoreError == nil {
		c.IgnoreConfig.ShouldIgnoreError = c.ShouldIgnoreError
	}

	// default ignore and retry configs to table defaults
	c.RetryConfig.DefaultTo(table.DefaultRetryConfig)
	c.IgnoreConfig.DefaultTo(table.DefaultIgnoreConfig)

	// if cost is not set, initialise to 1
	if c.Cost == 0 {
		log.Printf("[TRACE] Cost is not set - defaulting to 1")
		c.Cost = 1
	}
	log.Printf("[TRACE] HydrateConfig.initialise complete: RetryConfig: %s, IgnoreConfig: %s", c.RetryConfig.String(), c.IgnoreConfig.String())
}

func (c *HydrateConfig) Validate(table *Table) []string {
	var validationErrors []string
	if c.Func == nil {
		validationErrors = append(validationErrors, fmt.Sprintf("table '%s' HydrateConfig does not specify a hydrate function", table.Name))
	}

	if c.RetryConfig != nil {
		validationErrors = append(validationErrors, c.RetryConfig.validate(table)...)
	}
	if c.IgnoreConfig != nil {
		validationErrors = append(validationErrors, c.IgnoreConfig.validate(table)...)
	}
	return validationErrors
}

//func (c *HydrateConfig) GetRateLimit() rate.Limit {
//	if c.RateLimit != nil {
//		return c.RateLimit.Limit
//	}
//	return rate_limiter.GetDefaultHydrateRate()
//}
//
//func (c *HydrateConfig) GetRateLimitBurst() int {
//	if c.RateLimit != nil {
//		return c.RateLimit.Burst
//	}
//
//	return rate_limiter.GetDefaultHydrateBurstSize()
//}

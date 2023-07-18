package plugin

import (
	"fmt"
	"log"
	"strings"

	"github.com/turbot/go-kit/helpers"
)

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
	Func HydrateFunc
	// a function which will return whenther to ignore a given error
	IgnoreConfig *IgnoreConfig
	// a function which will return whenther to retry the call if an error is returned
	RetryConfig *RetryConfig
	Depends     []HydrateFunc
	RateLimit   *HydrateRateLimiterConfig

	// Deprecated: use IgnoreConfig
	ShouldIgnoreError ErrorPredicate
	// Deprecated: use RateLimit.MaxConcurrency
	MaxConcurrency int
}

func (c *HydrateConfig) String() interface{} {
	var dependsStrings = make([]string, len(c.Depends))
	for i, dep := range c.Depends {
		dependsStrings[i] = helpers.GetFunctionName(dep)
	}
	str := fmt.Sprintf(`Func: %s
RetryConfig: %s
IgnoreConfig: %s
Depends: %s
Rate Limit: %s`,
		c.RetryConfig,
		c.IgnoreConfig,
		strings.Join(dependsStrings, ","),
		c.RateLimit)

	return str
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

	// create empty RateLimiter config if needed
	if c.RateLimit == nil {
		c.RateLimit = &HydrateRateLimiterConfig{}
	}
	// initialise the rate limit config
	// this adds the hydrate name into the tag map and initializes cost
	c.RateLimit.initialise(c.Func)

	// copy the (deprecated) top level ShouldIgnoreError property into the ignore config
	if c.IgnoreConfig.ShouldIgnoreError == nil {
		c.IgnoreConfig.ShouldIgnoreError = c.ShouldIgnoreError
	}

	// copy the (deprecated) top level ShouldIgnoreError property into the ignore config
	if c.MaxConcurrency != 0 && c.RateLimit.MaxConcurrency == 0 {
		c.RateLimit.MaxConcurrency = c.MaxConcurrency
		// if we the config DOES NOT define both the new and deprected property, clear the deprectaed property
		// this way - the validation will not raise an error if ONLY the deprecated property is set
		c.MaxConcurrency = 0
	}
	// if both are set, leave both set - we will get a validation error

	// default ignore and retry configs to table defaults
	c.RetryConfig.DefaultTo(table.DefaultRetryConfig)
	c.IgnoreConfig.DefaultTo(table.DefaultIgnoreConfig)

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
	validationErrors = append(validationErrors, c.RateLimit.validate()...)

	if c.MaxConcurrency != 0 && c.RateLimit.MaxConcurrency != 0 {
		validationErrors = append(validationErrors, fmt.Sprintf("table '%s' HydrateConfig contains both deprecated 'MaxConcurrency' and the replacement 'RateLimit.MaxConcurrency", table.Name))
	}

	return validationErrors
}

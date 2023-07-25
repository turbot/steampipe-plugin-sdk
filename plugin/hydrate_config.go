package plugin

import (
	"fmt"
	"log"
	"strings"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
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
	// static scope values used to resolve the rate limiter for this hydrate call
	// for example:
	// "service": "s3"
	//
	// when resolving a rate limiter for a hydrate call, a map of scope values is automatically populated:
	// - the plugin, table, connection and hydrate func name
	// - values specified in the hydrate config
	// - quals (with values as string)
	// this map is then used to find a rate limiter
	ScopeValues map[string]string
	// how expensive is this hydrate call
	// roughly - how many API calls does it hit
	Cost int

	MaxConcurrency int

	// Deprecated: use IgnoreConfig
	ShouldIgnoreError ErrorPredicate
}

func (c *HydrateConfig) String() string {
	var dependsStrings = make([]string, len(c.Depends))
	for i, dep := range c.Depends {
		dependsStrings[i] = helpers.GetFunctionName(dep)
	}
	str := fmt.Sprintf(`Func: %s
RetryConfig: %s
IgnoreConfig: %s
Depends: %s
ScopeValues: %s
Cost: %d`,
		helpers.GetFunctionName(c.Func),
		c.RetryConfig,
		c.IgnoreConfig,
		strings.Join(dependsStrings, ","),
		rate_limiter.FormatStringMap(c.ScopeValues),
		c.Cost)

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

	// create empty ScopeValues if needed
	if c.ScopeValues == nil {
		c.ScopeValues = map[string]string{}
	}
	// if cost is not set, initialise to 1
	if c.Cost == 0 {
		log.Printf("[TRACE] HydrateConfig initialise - cost is not set - defaulting to 1")
		c.Cost = 1
	}
	// copy the (deprecated) top level ShouldIgnoreError property into the ignore config
	if c.IgnoreConfig.ShouldIgnoreError == nil {
		c.IgnoreConfig.ShouldIgnoreError = c.ShouldIgnoreError
	}

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

	return validationErrors
}

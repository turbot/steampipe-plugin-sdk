package plugin

import (
	"fmt"
	"log"
	"strings"

	"github.com/turbot/go-kit/helpers"
)

/*
HydrateConfig defines how to run a [HydrateFunc]:

  - which errors to ignore: [plugin.HydrateConfig.IgnoreConfig]
  - which errors to retry: [plugin.HydrateConfig.RetryConfig]
  - how many concurrent calls to allow: [plugin.HydrateConfig.MaxConcurrency]
  - which hydrate calls must complete before this HydrateFunc can start: [plugin.HydrateConfig.Depends]

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
		},
		}

Plugin examples:
  - [oci]

[oci]: https://github.com/turbot/steampipe-plugin-oci/blob/27ddf689f7606009cf26b2716e1634fc91d53585/oci/table_oci_identity_tenancy.go#L23-L27
*/
type HydrateConfig struct {
	Func           HydrateFunc
	MaxConcurrency int
	RetryConfig    *RetryConfig
	IgnoreConfig   *IgnoreConfig
	// deprecated - use IgnoreConfig
	ShouldIgnoreError ErrorPredicate

	Depends []HydrateFunc
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

	log.Printf("[TRACE] HydrateConfig.initialise complete: RetryConfig: %s, IgnoreConfig: %s", c.RetryConfig.String(), c.IgnoreConfig.String())
}

func (c *HydrateConfig) Validate(table *Table) []string {
	var validationErrors []string
	if c.Func == nil {
		validationErrors = append(validationErrors, fmt.Sprintf("table '%s' HydrateConfig does not specify a hydrate function", table.Name))
	}

	if c.RetryConfig != nil {
		validationErrors = append(validationErrors, c.RetryConfig.Validate(table)...)
	}
	if c.IgnoreConfig != nil {
		validationErrors = append(validationErrors, c.IgnoreConfig.Validate(table)...)
	}
	return validationErrors
}

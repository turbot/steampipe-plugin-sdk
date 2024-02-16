package plugin

import (
	"fmt"
	"log"

	"github.com/gertd/go-pluralize"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
)

/*
A GetConfig defines how to get a single row of a table:

  - The [key_columns] that uniquely identify a row.

  - The [error_handling] behaviour.

  - How many concurrent [HydrateFunc] calls to allow, [when the get call is used as a column hydrate func]: [plugin.GetConfig.MaxConcurrency].

A GetConfig with KeyColumns:

	Get: &plugin.GetConfig{
		KeyColumns: plugin.SingleColumn("id"),
		Hydrate:    getItem,
	}

A GetConfig with IgnoreConfig:

	Get: &plugin.GetConfig{
		KeyColumns: 	plugin.SingleColumn("id"),
		Hydrate:    	getItem,
		IgnoreConfig:   &plugin.IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreError},
	}

A GetConfig with RetryConfig:

	Get: &plugin.GetConfig{
		KeyColumns: 	plugin.SingleColumn("id"),
		Hydrate:    	getItem,
		RetryConfig:    &plugin.RetryConfig{
			ShouldRetryErrorFunc: shouldRetryError,
		},
	}

A GetConfig with all fields specified:

	Get: &plugin.GetConfig{
		KeyColumns:     plugin.SingleColumn("id"),
		Hydrate:        getItem,
		RetryConfig:    &plugin.RetryConfig{
			ShouldRetryErrorFunc: shouldRetryError,
		},
		IgnoreConfig:   &plugin.IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreError},
		MaxConcurrency: 50,
	}

Plugin examples:
  - [hackernews]

[hackernews]: https://github.com/turbot/steampipe-plugin-hackernews/blob/bbfbb12751ad43a2ca0ab70901cde6a88e92cf44/hackernews/table_hackernews_item.go#L21-L24
[when the get call is used as a column hydrate func]: https://github.com/turbot/steampipe-plugin-hackernews/blob/d14efdd3f2630f0146e575fe07666eda4e126721/hackernews/item.go#L14-L35
*/
type GetConfig struct {
	// the hydrate function which is called first when performing a 'get' call.
	// if this returns 'not found', no further hydrate functions are called
	Hydrate HydrateFunc
	// key or keys which are used to uniquely identify rows - used to determine whether  a query is a 'get' call
	KeyColumns KeyColumnSlice
	// a function which will return whenther to ignore a given error
	IgnoreConfig *IgnoreConfig
	// a function which will return whenther to retry the call if an error is returned
	RetryConfig *RetryConfig
	Tags        map[string]string

	// Deprecated: use IgnoreConfig
	ShouldIgnoreError ErrorPredicate
	MaxConcurrency    int
	namedHydrate      namedHydrateFunc
}

// initialise the GetConfig
// if table is NOT passed, this is the plugin default
// - ensure RetryConfig and IgnoreConfig are non null
// - handle use of deprecated ShouldIgnoreError
// - if a table was passed (i.e. this is the get config for a specific table), apply plugin level defaults
func (c *GetConfig) initialise(table *Table) {
	if table != nil {
		log.Printf("[TRACE] GetConfig.initialise table %s", table.Name)
	} else {
		log.Printf("[TRACE] GetConfig.initialise (plugin default)")
	}

	// create RetryConfig if needed
	if c.RetryConfig == nil {
		c.RetryConfig = &RetryConfig{}
	}

	// create DefaultIgnoreConfig if needed
	if c.IgnoreConfig == nil {
		c.IgnoreConfig = &IgnoreConfig{}
	}

	// create empty tags if needed
	if c.Tags == nil {
		c.Tags = make(map[string]string)
	}
	// add in function name to tags
	c.Tags[rate_limiter.RateLimiterScopeFunction] = c.namedHydrate.Name

	// copy the (deprecated) top level ShouldIgnoreError property into the ignore config
	if c.IgnoreConfig.ShouldIgnoreError == nil {
		c.IgnoreConfig.ShouldIgnoreError = c.ShouldIgnoreError
	}

	// if a table was passed (i.e. this is NOT the plugin default)
	// default ignore and retry configs
	if table != nil {
		// if there is a default get config, default to its ignore and retry config (if they exist)
		if defaultGetConfig := table.Plugin.DefaultGetConfig; defaultGetConfig != nil {
			c.RetryConfig.DefaultTo(defaultGetConfig.RetryConfig)
			c.IgnoreConfig.DefaultTo(defaultGetConfig.IgnoreConfig)
		}
		// then default to the table default
		c.RetryConfig.DefaultTo(table.DefaultRetryConfig)
		c.IgnoreConfig.DefaultTo(table.DefaultIgnoreConfig)
	}
	log.Printf("[TRACE] GetConfig.initialise complete: RetryConfig: %s, IgnoreConfig: %s", c.RetryConfig.String(), c.IgnoreConfig.String())

	// create a named hydrate func
	c.namedHydrate = newNamedHydrateFunc(c.Hydrate)
}

func (c *GetConfig) Validate(table *Table) []string {
	var validationErrors []string

	if c.Hydrate == nil {
		validationErrors = append(validationErrors, fmt.Sprintf("table '%s' GetConfig does not specify a hydrate function", table.Name))
	}
	if c.KeyColumns == nil {
		validationErrors = append(validationErrors, fmt.Sprintf("table '%s' GetConfig does not specify a KeyColumn", table.Name))
	}
	if c.RetryConfig != nil {
		validationErrors = append(validationErrors, c.RetryConfig.validate(table)...)
	}
	if c.IgnoreConfig != nil {
		validationErrors = append(validationErrors, c.IgnoreConfig.validate(table)...)
	}
	// ensure that if there is an explicit hydrate config for the get hydrate, it does not declare dependencies
	getHydrateName := helpers.GetFunctionName(table.Get.Hydrate)
	for _, h := range table.HydrateConfig {
		if helpers.GetFunctionName(h.Func) == getHydrateName {
			if len(h.Depends) > 0 {
				validationErrors = append(validationErrors, fmt.Sprintf("table '%s' Get hydrate function '%s' defines dependendencies in its `HydrateConfig`", table.Name, getHydrateName))
			}
			break
		}
	}

	// ensure there is no hydrate dependency declared for the get hydrate
	for _, h := range table.HydrateDependencies {
		if helpers.GetFunctionName(h.Func) == getHydrateName {
			numDeps := len(h.Depends)
			validationErrors = append(validationErrors, fmt.Sprintf("table '%s' Get hydrate function '%s' has %d %s - Get hydrate functions cannot have dependencies",
				table.Name,
				getHydrateName,
				numDeps,
				pluralize.NewClient().Pluralize("dependency", numDeps, false)))
			break
		}
	}

	return validationErrors
}

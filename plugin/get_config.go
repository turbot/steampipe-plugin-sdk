package plugin

import (
	"fmt"
	"log"

	"github.com/turbot/go-kit/helpers"
)

// GetConfig is a struct used to define the configuration of the table 'Get' function.
// This is the function used to retrieve a single row by id
// The config defines the function, the columns which may be used as id (KeyColumns), and the error handling behaviour
type GetConfig struct {
	// key or keys which are used to uniquely identify rows - used to determine whether  a query is a 'get' call
	KeyColumns KeyColumnSlice
	// the hydrate function which is called first when performing a 'get' call.
	// if this returns 'not found', no further hydrate functions are called
	Hydrate HydrateFunc
	// a function which will return whenther to ignore a given error
	// deprecated - use IgnoreConfig
	ShouldIgnoreError ErrorPredicate
	IgnoreConfig      *IgnoreConfig
	RetryConfig       *RetryConfig
}

func (c *GetConfig) initialise(table *Table) {
	log.Printf("[TRACE] GetConfig.initialise table %s", table.Name)
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

	// default ignore and retry configs

	// if there is a default get config, default to its ignore and retry config (if they exist)
	if defaultGetConfig := table.Plugin.DefaultGetConfig; defaultGetConfig != nil {
		c.RetryConfig.DefaultTo(defaultGetConfig.RetryConfig)
		c.IgnoreConfig.DefaultTo(defaultGetConfig.IgnoreConfig)
	}
	// then default to the table default
	c.RetryConfig.DefaultTo(table.DefaultRetryConfig)
	c.IgnoreConfig.DefaultTo(table.DefaultIgnoreConfig)

	log.Printf("[TRACE] GetConfig.initialise complete: RetryConfig: %s, IgnoreConfig: %s", c.RetryConfig.String(), c.IgnoreConfig.String())
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
		validationErrors = append(validationErrors, c.RetryConfig.Validate(table)...)
	}
	if c.IgnoreConfig != nil {
		validationErrors = append(validationErrors, c.IgnoreConfig.Validate(table)...)
	}
	// ensure there is no explicit hydrate config for the get config
	getHydrateName := helpers.GetFunctionName(table.Get.Hydrate)
	for _, h := range table.HydrateConfig {
		if helpers.GetFunctionName(h.Func) == getHydrateName {
			validationErrors = append(validationErrors, fmt.Sprintf("table '%s' Get hydrate function '%s' also has an explicit hydrate config declared in `HydrateConfig`", table.Name, getHydrateName))
			break
		}
	}

	return validationErrors
}

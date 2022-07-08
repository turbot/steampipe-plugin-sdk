package plugin

import (
	"fmt"
	"log"
	"strings"

	"github.com/turbot/go-kit/helpers"
)

// HydrateConfig defines the hydrate function configurations, including the function name, maximum number of concurrent calls to be allowed, retry and ignore config, and dependencies.
// The function in GetConfig cannot have a separate HydrateConfig. Instead, please define any configurations directly in GetConfig.
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

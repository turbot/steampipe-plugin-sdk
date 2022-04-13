package plugin

import (
	"fmt"
	"strings"

	"github.com/turbot/go-kit/helpers"
)

// HydrateConfig defines the hydrate function configurations, Name, Maximum number of concurrent calls to be allowed, dependencies
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
	// create RetryConfig if needed
	if c.RetryConfig == nil {
		c.RetryConfig = &RetryConfig{}
	}

	// create DefaultIgnoreConfig if needed
	if c.IgnoreConfig == nil {
		c.IgnoreConfig = &IgnoreConfig{}
	}
	// copy the (deprecated) top level ShouldIgnoreError property into the ignore config
	c.IgnoreConfig.ShouldIgnoreError = c.ShouldIgnoreError

	// default ignore and retry configs to table defaults
	c.RetryConfig.DefaultTo(table.DefaultRetryConfig)
	c.IgnoreConfig.DefaultTo(table.DefaultIgnoreConfig)
}

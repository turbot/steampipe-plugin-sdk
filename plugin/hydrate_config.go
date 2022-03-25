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

	ShouldIgnoreError ErrorPredicate
	Depends           []HydrateFunc
}

func (c *HydrateConfig) DefaultTo(defaultConfig *HydrateConfig) {
	if defaultConfig == nil {
		return
	}
	if c.RetryConfig == nil {
		c.RetryConfig = defaultConfig.RetryConfig
	}
	if c.Depends == nil {
		c.Depends = defaultConfig.Depends
	}
}

func (c *HydrateConfig) String() interface{} {
	shouldIgnoreErrorString := ""
	if c.ShouldIgnoreError != nil {
		shouldIgnoreErrorString = helpers.GetFunctionName(c.Func)
	}
	var dependsStrings = make([]string, len(c.Depends))
	for i, dep := range c.Depends {
		dependsStrings[i] = helpers.GetFunctionName(dep)
	}
	return fmt.Sprintf(`Func: %s
MaxConcurrency: %d
RetryConfig: %s
ShouldIgnoreError: %s
Depends: %s`,
		helpers.GetFunctionName(c.Func),
		c.MaxConcurrency,
		c.RetryConfig.String(),
		shouldIgnoreErrorString,
		strings.Join(dependsStrings, ","))
}

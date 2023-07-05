package plugin

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/turbot/go-kit/helpers"
)

/*
RetryConfig retries [HydrateFunc] errors.

If a HydrateFunc returns an error in the first attempt but resolves itself in a future attempt, for instance API rate limit or throttling errors, set [plugin.GetConfig.RetryConfig], [plugin.ListConfig.RetryConfig] or [plugin.HydrateConfig.RetryConfig].

For errors common to many HydrateFuncs, you can define a default RetryConfig by setting [plugin.DefaultGetConfig].

Retry errors from a HydrateFunc that has a GetConfig:

	Get: &plugin.GetConfig{
		RetryConfig: &plugin.RetryConfig{
			ShouldRetryError: shouldRetryError,
		},
		...
	},

Retry errors from a HydrateFunc that has a ListConfig:

	List: &plugin.ListConfig{
		RetryConfig: &plugin.RetryConfig{
			ShouldRetryError: shouldRetryError,
		},
		...
	},

Retry errors from a HydrateFunc that has a HydrateConfig:

	HydrateConfig: []plugin.HydrateConfig{
		RetryConfig: &plugin.RetryConfig{
			ShouldRetryError: shouldRetryError,
		},
		...
	},

Retry errors that may occur in many HydrateFuncs:

	DefaultIgnoreConfig: &plugin.DefaultIgnoreConfig{
		RetryConfig: &plugin.RetryConfig{
			ShouldRetryError: shouldRetryError,
		},
		...
	},
*/
type RetryConfig struct {
	ShouldRetryErrorFunc ErrorPredicateWithContext
	// deprecated use ShouldRetryErrorFunc
	ShouldRetryError ErrorPredicate

	// Maximum number of retry operation to be performed. Default set to 10.
	MaxAttempts int64
	// Algorithm for the backoff. Supported values: Fibonacci, Exponential, and Constant. Default set to Fibonacci.
	BackoffAlgorithm string
	// Starting interval. Default set to 100ms.
	RetryInterval int64
	// Set a maximum on the duration (in ms) returned from the next backoff.
	CappedDuration int64
	// Sets a maximum on the total amount of time (in ms) a backoff should execute.
	MaxDuration int64
}

func (c *RetryConfig) String() interface{} {
	if c.ShouldRetryError != nil {
		return fmt.Sprintf("ShouldRetryError: %s", helpers.GetFunctionName(c.ShouldRetryError))
	}
	if c.ShouldRetryErrorFunc != nil {
		return fmt.Sprintf("ShouldRetryErrorFunc: %s", helpers.GetFunctionName(c.ShouldRetryErrorFunc))
	}
	return ""
}

func (c *RetryConfig) validate(table *Table) []string {
	var res []string
	validBackoffAlgorithm := []string{"Constant", "Exponential", "Fibonacci"}

	var tablePrefix string
	if table != nil {
		tablePrefix = fmt.Sprintf("table '%s': ", table.Name)
	}
	if c.ShouldRetryError != nil && c.ShouldRetryErrorFunc != nil {
		log.Printf("[WARN] RetryConfig validate failed - both ShouldRetryError and ShouldRetryErrorFunc are defined")
		res = append(res, fmt.Sprintf("%sboth ShouldRetryError and ShouldRetryErrorFunc are defined", tablePrefix))
	}

	if c.BackoffAlgorithm != "" && !helpers.StringSliceContains(validBackoffAlgorithm, c.BackoffAlgorithm) {
		res = append(res, fmt.Sprintf("%sBackoffAlgorithm value '%s' is not valid, it must be one of: %s", tablePrefix, c.BackoffAlgorithm, strings.Join(validBackoffAlgorithm, ",")))
	}

	return res
}

func (c *RetryConfig) DefaultTo(other *RetryConfig) {
	// if not other provided, nothing to do
	if other == nil {
		return
	}
	// if either ShouldIgnoreError or ShouldRetryErrorFunc are set, do not default to other
	if c.ShouldRetryError != nil || c.ShouldRetryErrorFunc != nil {
		log.Printf("[TRACE] RetryConfig DefaultTo: config defines a should retry function so not defaulting to base")
		return
	}

	// legacy func
	if c.ShouldRetryError == nil && other.ShouldRetryError != nil {
		log.Printf("[TRACE] RetryConfig DefaultTo: using base ShouldRetryError: %s", helpers.GetFunctionName(other.ShouldRetryError))
		c.ShouldRetryError = other.ShouldRetryError
	}
	if c.ShouldRetryErrorFunc == nil && other.ShouldRetryErrorFunc != nil {
		log.Printf("[TRACE] RetryConfig DefaultTo: using base ShouldRetryErrorFunc: %s", helpers.GetFunctionName(other.ShouldRetryErrorFunc))
		c.ShouldRetryErrorFunc = other.ShouldRetryErrorFunc
	}
}

// GetListRetryConfig  wraps the ShouldRetry function with an additional check of the rows streamed
// (as we cannot retry errors in the list hydrate function after streaming has started)
func (c *RetryConfig) GetListRetryConfig() *RetryConfig {
	listRetryConfig := &RetryConfig{}
	if c.ShouldRetryErrorFunc != nil {
		listRetryConfig.ShouldRetryErrorFunc = func(ctx context.Context, d *QueryData, h *HydrateData, err error) bool {
			if d.queryStatus.rowsStreamed != 0 {
				log.Printf("[TRACE] shouldRetryError we have started streaming rows (%d) - return false", d.queryStatus.rowsStreamed)
				return false
			}
			res := c.ShouldRetryErrorFunc(ctx, d, h, err)
			return res
		}
	} else if c.ShouldRetryError != nil {
		listRetryConfig.ShouldRetryErrorFunc = func(ctx context.Context, d *QueryData, h *HydrateData, err error) bool {
			if d.queryStatus.rowsStreamed != 0 {
				log.Printf("[TRACE] shouldRetryError we have started streaming rows (%d) - return false", d.queryStatus.rowsStreamed)
				return false
			}
			// call the legacy function
			return c.ShouldRetryError(err)
		}
	}
	return listRetryConfig
}

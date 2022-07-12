package plugin

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/turbot/go-kit/helpers"
)

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

func (c *RetryConfig) Validate(table *Table) []string {
	var res []string
	validBackoffAlgorithm := []string{"Constant", "Exponential", "Fibonacci"}

	if c.ShouldRetryError != nil && c.ShouldRetryErrorFunc != nil {
		log.Printf("[TRACE] RetryConfig validate failed - both ShouldRetryError and ShouldRetryErrorFunc are defined")
		var tablePrefix string
		if table != nil {
			tablePrefix = fmt.Sprintf("table '%s' ", table.Name)
		}

		res = append(res, fmt.Sprintf("%sboth ShouldRetryError and ShouldRetryErrorFunc are defined", tablePrefix))
	}

	if c.BackoffAlgorithm != "" && !helpers.StringSliceContains(validBackoffAlgorithm, c.BackoffAlgorithm) {
		res = append(res, fmt.Sprintf("BackoffAlgorithm value '%s' is not valid, it must be one of: %s", c.BackoffAlgorithm, strings.Join(validBackoffAlgorithm, ",")))
	}

	return res
}

func (c *RetryConfig) DefaultTo(other *RetryConfig) {
	// if not other provided, nothing to do
	if other == nil {
		return
	}
	// if either ShouldIgnoreError or ShouldIgnoreErrorFunc are set, do not default to other
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
			if d.QueryStatus.rowsStreamed != 0 {
				log.Printf("[TRACE] shouldRetryError we have started streaming rows (%d) - return false", d.QueryStatus.rowsStreamed)
				return false
			}
			res := c.ShouldRetryErrorFunc(ctx, d, h, err)
			return res
		}
	} else if c.ShouldRetryError != nil {
		listRetryConfig.ShouldRetryErrorFunc = func(ctx context.Context, d *QueryData, h *HydrateData, err error) bool {
			if d.QueryStatus.rowsStreamed != 0 {
				log.Printf("[TRACE] shouldRetryError we have started streaming rows (%d) - return false", d.QueryStatus.rowsStreamed)
				return false
			}
			// call the legacy function
			return c.ShouldRetryError(err)
		}
	}
	return listRetryConfig
}

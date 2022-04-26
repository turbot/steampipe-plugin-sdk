package plugin

import (
	"fmt"
	"log"

	"github.com/turbot/go-kit/helpers"
)

type RetryConfig struct {
	ShouldRetryError     ErrorPredicate
	ShouldRetryErrorFunc ErrorPredicateWithContext
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
	if c.ShouldRetryError != nil && c.ShouldRetryErrorFunc != nil {
		log.Printf("[TRACE] RetryConfig validate failed - both ShouldRetryError and ShouldRetryErrorFunc are defined")
		var tablePrefix string
		if table != nil {
			tablePrefix = fmt.Sprintf("table '%s' ", table.Name)
		}

		return []string{fmt.Sprintf("%sboth ShouldRetryError and ShouldRetryErrorFunc are defined", tablePrefix)}
	}
	return nil
}

func (c *RetryConfig) DefaultTo(other *RetryConfig) {
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

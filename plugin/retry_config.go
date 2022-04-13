package plugin

import (
	"fmt"

	"github.com/turbot/go-kit/helpers"
)

type RetryConfig struct {
	ShouldRetryError     ErrorPredicate
	ShouldRetryErrorFunc ErrorPredicateWithContext
}

func (c RetryConfig) String() interface{} {
	if c.ShouldRetryError != nil {
		return fmt.Sprintf("ShouldRetryError: %s", helpers.GetFunctionName(c.ShouldRetryError))
	}
	if c.ShouldRetryErrorFunc != nil {
		return fmt.Sprintf("ShouldRetryErrorFunc: %s", helpers.GetFunctionName(c.ShouldRetryErrorFunc))
	}
	return ""
}

func (c RetryConfig) Validate() []string {
	if c.ShouldRetryError != nil && c.ShouldRetryErrorFunc != nil {
		return []string{"both ShouldRetryError and ShouldRetryErrorFunc are defined"}
	}
	return nil
}

func (c RetryConfig) DefaultTo(other *RetryConfig) {
	// legacy func
	if c.ShouldRetryError == nil {
		c.ShouldRetryError = other.ShouldRetryError
	}
	if c.ShouldRetryErrorFunc == nil {
		c.ShouldRetryErrorFunc = other.ShouldRetryErrorFunc
	}
}

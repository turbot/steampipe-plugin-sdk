package plugin

import (
	"fmt"

	"github.com/turbot/go-kit/helpers"
)

type IgnoreConfig struct {
	ShouldIgnoreError     ErrorPredicate
	ShouldIgnoreErrorFunc ErrorPredicateWithContext
}

func (c IgnoreConfig) String() interface{} {
	if c.ShouldIgnoreError != nil {
		return fmt.Sprintf("ShouldIgnoreError: %s", helpers.GetFunctionName(c.ShouldIgnoreError))
	}
	if c.ShouldIgnoreErrorFunc != nil {
		return fmt.Sprintf("ShouldIgnoreErrorFunc: %s", helpers.GetFunctionName(c.ShouldIgnoreErrorFunc))
	}
	return ""
}

func (c IgnoreConfig) Validate() []string {
	if c.ShouldIgnoreError != nil && c.ShouldIgnoreErrorFunc != nil {
		return []string{"both ShouldIgnoreError and ShouldIgnoreErrorFunc are defined"}
	}
	return nil
}

func (c IgnoreConfig) DefaultTo(other *IgnoreConfig) {
	// legacy func
	if c.ShouldIgnoreError == nil {
		c.ShouldIgnoreError = other.ShouldIgnoreError
	}
	if c.ShouldIgnoreErrorFunc == nil {
		c.ShouldIgnoreErrorFunc = other.ShouldIgnoreErrorFunc
	}
}

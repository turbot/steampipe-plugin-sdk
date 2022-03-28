package plugin

import (
	"fmt"

	"github.com/turbot/go-kit/helpers"
)

type RetryConfig struct {
	ShouldRetryError ErrorPredicate
}

func (c RetryConfig) String() interface{} {
	return fmt.Sprintf("ShouldRetryError: %s", helpers.GetFunctionName(c.ShouldRetryError))
}

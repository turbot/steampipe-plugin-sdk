package error_helpers

import (
	"context"
	"errors"
	"strings"
)

func IsContextCancelledError(err error) bool {
	// hacky - strings.Contains is in to handle rpc error - better to try to cast
	return err != nil && (errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled"))
}

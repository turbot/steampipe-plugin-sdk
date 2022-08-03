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

// IsCancelled is a helper function which returns whether the context has been cancelled
func IsCancelled(ctx context.Context) bool {
	return IsContextCancelledError(ctx.Err())
}

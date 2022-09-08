package plugin

import (
	"context"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/v4/error_helpers"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/context_key"
)

// Logger extracts the logger from the context
func Logger(ctx context.Context) hclog.Logger {
	return ctx.Value(context_key.Logger).(hclog.Logger)
}

// GetMatrixItem extracts the matrix item map with the given key from the context
func GetMatrixItem(ctx context.Context) map[string]interface{} {
	value := ctx.Value(context_key.MatrixItem)
	if value == nil {
		return nil
	}
	return value.(map[string]interface{})
}

// IsCancelled is a helper function which returns whether the context has been cancelled
func IsCancelled(ctx context.Context) bool {
	return error_helpers.IsContextCancelledError(ctx.Err())
}

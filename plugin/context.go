package plugin

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/context_key"

	"github.com/hashicorp/go-hclog"
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

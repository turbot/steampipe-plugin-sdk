package plugin

import (
	"context"
	"errors"

	"github.com/turbot/steampipe-plugin-sdk/plugin/context_key"

	"github.com/hashicorp/go-hclog"
)

func Logger(ctx context.Context) hclog.Logger {
	return ctx.Value(context_key.Logger).(hclog.Logger)
}

func GetMatrixItem(ctx context.Context) map[string]interface{} {
	value := ctx.Value(context_key.MatrixItem)
	if value == nil {
		return nil
	}
	return value.(map[string]interface{})
}

func ContextCancelled(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.Canceled)
}

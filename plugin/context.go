package plugin

import (
	"context"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/v5/error_helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/context_key"
)

/*
Logger retrieves the [hclog.Logger] from the context.

Log at trace level:

	plugin.Logger(ctx).Trace("Code execution starts here")

Log at error level with single data:

	plugin.Logger(ctx).Error("hackernews_item.itemList", "query_error", err)

Log at info level with single data:

	plugin.Logger(ctx).Info("listGreeting", "number", i)

Log at warn level with multiple data:

	plugin.Logger(ctx).Warn("getDomain", "invalid_name", err, "query_response", resp)
*/
func Logger(ctx context.Context) hclog.Logger {
	return ctx.Value(context_key.Logger).(hclog.Logger)
}

// GetMatrixItem retrieves the matrix item from the context
func GetMatrixItem(ctx context.Context) map[string]interface{} {
	value := ctx.Value(context_key.MatrixItem)

	if value == nil {
		return nil
	}
	return value.(map[string]interface{})
}

/*
IsCancelled returns whether the context has been cancelled.

To use:

	for _, i := range items {
		d.StreamListItem(ctx, i)
		if plugin.IsCancelled(ctx) {
			return nil, nil
		}
	}

Plugin examples:
  - [heroku]

[heroku]: https://github.com/turbot/steampipe-plugin-heroku/blob/a811484d8e29d7478dd9d08adddf0f660563a8ea/heroku/table_heroku_key.go#L58
*/
func IsCancelled(ctx context.Context) bool {
	return error_helpers.IsContextCancelledError(ctx.Err())
}

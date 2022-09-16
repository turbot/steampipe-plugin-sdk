package plugin

import (
	"context"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/v4/error_helpers"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/context_key"
)

/*
Logger retrieves the [hclog.Logger] from the context.

Usage:

	plugin.Logger(ctx).Trace("Code execution starts here")
	plugin.Logger(ctx).Error("hackernews_item.itemList", "query_error", err)
	plugin.Logger(ctx).Warn("getDomain", "invalid_name", err, "query_response", resp)
	plugin.Logger(ctx).Info("listGreeting", "number", i)
*/
func Logger(ctx context.Context) hclog.Logger {
	return ctx.Value(context_key.Logger).(hclog.Logger)
}

/*
Deprecated: Please use [plugin.Table.GetMatrixItemFunc] instead.
*/
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

package plugin

import (
	"context"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/v4/error_helpers"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/context_key"
)

/* 
Logger function extracts the logs from the context. It is useful for debugging in the event of failures.

The logs can be visualized by using "STEAMPIPE_LOG=TRACE" command.

Sample code snippet available [here].

[here]: https://github.com/turbot/steampipe-plugin-hackernews/blob/d14efdd3f2630f0146e575fe07666eda4e126721/hackernews/table_hackernews_item.go#L32
*/
func Logger(ctx context.Context) hclog.Logger {
	return ctx.Value(context_key.Logger).(hclog.Logger)
}

/* 
GetMatrixItem extracts the matrix item map with the given key from the context.

This function has now been replaced by [GetMatrixItemFunc]
*/
func GetMatrixItem(ctx context.Context) map[string]interface{} {
	value := ctx.Value(context_key.MatrixItem)
	if value == nil {
		return nil
	}
	return value.(map[string]interface{})
}

/* 
IsCancelled is a helper function which returns whether the context has been cancelled.

It is instrumental in the event a context has been cancelled due to manual cancellation or the limit has been hit.

Sample code snippet available [here].

[here]: https://github.com/turbot/steampipe-plugin-heroku/blob/a811484d8e29d7478dd9d08adddf0f660563a8ea/heroku/table_heroku_key.go#L58
*/
func IsCancelled(ctx context.Context) bool {
	return error_helpers.IsContextCancelledError(ctx.Err())
}

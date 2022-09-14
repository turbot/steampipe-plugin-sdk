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

Example from [hackernews].

	func itemList(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
		resp, err := http.Get("https://hacker-news.firebaseio.com/v0/maxitem.json")
		if err != nil {
			plugin.Logger(ctx).Error("hackernews_item.itemList", "query_error", err)
			return nil, err
		}
	  ...
	}
[hackernews]: https://github.com/turbot/steampipe-plugin-hackernews/blob/d14efdd3f2630f0146e575fe07666eda4e126721/hackernews/table_hackernews_item.go#L32
*/
func Logger(ctx context.Context) hclog.Logger {
	return ctx.Value(context_key.Logger).(hclog.Logger)
}

/* 
GetMatrixItem extracts the matrix item map with the given key from the context.
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

Example from [hackernews].

[hackernews]: https://github.com/turbot/steampipe-plugin-heroku/blob/a811484d8e29d7478dd9d08adddf0f660563a8ea/heroku/table_heroku_key.go#L58
*/
func IsCancelled(ctx context.Context) bool {
	return error_helpers.IsContextCancelledError(ctx.Err())
}

package memoize

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"time"
)

// WithCacheKeyFunction sets the function used to build the cache key
func WithCacheKeyFunction(getCacheKeyFunc plugin.HydrateFunc) plugin.MemoizeOption {
	return func(o *plugin.MemoizeConfiguration) {
		o.GetCacheKeyFunc = getCacheKeyFunc
	}
}

// WithTtl sets memoize TTL
func WithTtl(ttl time.Duration) plugin.MemoizeOption {
	return func(o *plugin.MemoizeConfiguration) {
		o.Ttl = ttl
	}
}

package plugin

import (
	"context"
	"github.com/turbot/go-kit/helpers"
	"time"
)

type MemoizeConfiguration struct {
	GetCacheKeyFunc HydrateFunc
	Ttl             time.Duration
}

func newMemoizeConfiguration(hydrate HydrateFunc) *MemoizeConfiguration {
	var config = &MemoizeConfiguration{
		GetCacheKeyFunc: defaultGetHydrateCacheKeyFunc(hydrate),
		// default ttl to match existing connection cache default
		Ttl: time.Hour,
	}
	return config
}

func defaultGetHydrateCacheKeyFunc(hydrate HydrateFunc) HydrateFunc {
	return func(ctx context.Context, d *QueryData, h *HydrateData) (interface{}, error) {
		// no argument was supplied - infer cache key from the hydrate function
		return helpers.GetFunctionName(hydrate), nil
	}
}

type MemoizeOption = func(config *MemoizeConfiguration)

// MemoizeCacheKeyFunction sets the function used to build the cache key
func MemoizeCacheKeyFunction(getCacheKeyFunc HydrateFunc) MemoizeOption {
	return func(o *MemoizeConfiguration) {
		o.GetCacheKeyFunc = getCacheKeyFunc
	}
}

// MemoizeTtl sets the function used to build the cache key
func MemoizeTtl(ttl time.Duration) MemoizeOption {
	return func(o *MemoizeConfiguration) {
		o.Ttl = ttl
	}
}

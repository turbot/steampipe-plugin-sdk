package plugin

import (
	"context"
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"golang.org/x/exp/maps"
	"log"
	"strings"
)

type WithCacheConfiguration struct {
	GetCacheKeyFunc HydrateFunc
	hydrate         HydrateFunc
}

func (c WithCacheConfiguration) getCacheKeyFunction() HydrateFunc {
	if c.GetCacheKeyFunc == nil {
		c.GetCacheKeyFunc = defaultGetHydrateCacheKeyFunc(c.hydrate)
	}
	// TODO add connection name to user provided key
	return c.GetCacheKeyFunc
}

func defaultGetHydrateCacheKeyFunc(hydrate HydrateFunc) HydrateFunc {
	return func(ctx context.Context, d *QueryData, h *HydrateData) (interface{}, error) {
		funcName := helpers.GetFunctionName(hydrate)
		var matrixValueStr string
		// get all the matrix keys and get values for them if any
		if len(d.Matrix) > 0 {
			var matrixValues []string
			// assume all matrix items have the same keys
			matrixKeys := maps.Keys(d.Matrix[0])
			for _, k := range matrixKeys {
				if v := d.KeyColumnQualString(k); v != "" {
					matrixValues = append(matrixValues, v)
				}
			}
			if len(matrixValues) > 0 {
				matrixValueStr = fmt.Sprintf("-%s", strings.Join(matrixValues, "-"))
				log.Printf("[WARN] defaultGetHydrateCacheKeyFunc matrixValueStr %s", matrixValueStr)
			}
		}
		key := fmt.Sprintf("%s%s-%s", funcName, matrixValueStr, d.Connection.Name)
		log.Printf("[WARN] defaultGetHydrateCacheKeyFunc key %s", key)

		return key, nil
	}
}

type WithCacheOption = func(config *WithCacheConfiguration)

// WithCacheKeyFunction sets the function used to build the cache key
func WithCacheKeyFunction(getCacheKeyFunc HydrateFunc) WithCacheOption {
	return func(o *WithCacheConfiguration) {
		o.GetCacheKeyFunc = getCacheKeyFunc
	}
}

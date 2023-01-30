package plugin

import (
	"context"
)

// Memoize is a generic function which wraps the given hydrate function with Memoization,
// and also casts the return value to the desired type
// Note: the function return from this is NOT a HydrateFunc, but rather a function returning the desired type
// - this means it could not be used anywhere expecting a hydrate func, e.g. in a hydrate config etc.
func Memoize[T any](f HydrateFunc, opts ...MemoizeOption) func(ctx context.Context, d *QueryData, h *HydrateData) (T, error) {
	return func(ctx context.Context, d *QueryData, h *HydrateData) (T, error) {

		val, err := f.Memoize(opts...)(ctx, d, h)
		if err != nil {
			var res T
			return res, err

		}
		return val.(T), nil
	}
}

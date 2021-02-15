package plugin

import (
	"context"
	"fmt"
	"log"

	"github.com/turbot/go-kit/helpers"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrorPredicate func(error) bool

// SafeGet :: higher order function which returns a GetFunc which handles NotFound errors
func (t *Table) SafeGet() HydrateFunc {
	return func(ctx context.Context, d *QueryData, h *HydrateData) (item interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[WARN] SafeGet caught a panic: %v\n", r)
				err = status.Error(codes.Internal, fmt.Sprintf("get hydrate function %s failed with panic %v", helpers.GetFunctionName(t.Get.Hydrate), r))
			}
		}()
		// call the underlying get function
		item, err = t.Get.Hydrate(ctx, d, h)
		if err != nil {
			// suppress not found errors
			// see if either the table or thge plugin define a NotFoundErrorPredicate
			shouldIgnoreError := t.Get.ShouldIgnoreError
			if shouldIgnoreError == nil {
				if t.Plugin.DefaultGetConfig != nil {
					shouldIgnoreError = t.Plugin.DefaultGetConfig.ShouldIgnoreError
				}
			}
			log.Printf("[TRACE] SafeGet get call returned error %v\n", err)
			if shouldIgnoreError != nil && shouldIgnoreError(err) {
				log.Printf("[TRACE] get() returned error but we are ignoring it: %v", err)
				return nil, nil
			}
			// pass any other error on
			return nil, err
		}

		return item, nil
	}
}

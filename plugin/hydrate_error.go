package plugin

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/sethvargo/go-retry"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryHydrate function invokes the hydrate function with retryable errors and retries the function until the maximum attempts before throwing error
func RetryHydrate(ctx context.Context, d *QueryData, hydrateData *HydrateData, hydrateFunc HydrateFunc, retryConfig *RetryConfig) (hydrateResult interface{}, err error) {
	ctx, span := telemetry.StartSpan(ctx, d.Table.Plugin.Name, "RetryHydrate (%s)", d.Table.Name)
	span.SetAttributes(
		attribute.String("hydrate-func", helpers.GetFunctionName(hydrateFunc)),
	)
	defer func() {
		if err != nil {
			span.SetAttributes(
				attribute.String("err", err.Error()),
			)
		}
		span.End()
	}()

	backoff, err := retry.NewFibonacci(100 * time.Millisecond)
	if err != nil {
		return nil, err
	}

	err = retry.Do(ctx, retry.WithMaxRetries(10, backoff), func(ctx context.Context) error {
		hydrateResult, err = hydrateFunc(ctx, d, hydrateData)
		if err != nil {
			if shouldRetryError(ctx, d, hydrateData, err, retryConfig) {
				err = retry.RetryableError(err)
			}
		}
		return err
	})

	return hydrateResult, err
}

// WrapHydrate is a higher order function which returns a HydrateFunc which handles Ignorable errors
func WrapHydrate(hydrateFunc HydrateFunc, ignoreConfig *IgnoreConfig) HydrateFunc {
	return func(ctx context.Context, d *QueryData, h *HydrateData) (item interface{}, err error) {
		ctx, span := telemetry.StartSpan(ctx, d.Table.Plugin.Name, "hydrateWithIgnoreError (%s)", d.Table.Name)
		span.SetAttributes(
			attribute.String("hydrate-func", helpers.GetFunctionName(hydrateFunc)),
		)
		defer func() {
			if err != nil {
				span.SetAttributes(
					attribute.String("err", err.Error()),
				)
			}
			span.End()
		}()

		defer func() {
			if r := recover(); r != nil {
				log.Printf("[WARN] recovered a panic from a wrapped hydrate function: %v\n", r)
				err = status.Error(codes.Internal, fmt.Sprintf("hydrate function %s failed with panic %v", helpers.GetFunctionName(hydrateFunc), r))
			}
		}()
		// call the underlying get function
		item, err = hydrateFunc(ctx, d, h)
		if err != nil {
			log.Printf("[TRACE] wrapped hydrate call %s returned error %v, ignore config %s\n", helpers.GetFunctionName(hydrateFunc), err, ignoreConfig.String())
			// see if the ignoreConfig defines a should ignore function
			if ignoreConfig.ShouldIgnoreError != nil && ignoreConfig.ShouldIgnoreError(err) {
				log.Printf("[TRACE] wrapped hydrate call %s returned error but we are ignoring it: %v", helpers.GetFunctionName(hydrateFunc), err)
				return nil, nil
			}
			if ignoreConfig.ShouldIgnoreErrorFunc != nil && ignoreConfig.ShouldIgnoreErrorFunc(ctx, d, h, err) {
				log.Printf("[TRACE] wrapped hydrate call %s returned error but we are ignoring it: %v", helpers.GetFunctionName(hydrateFunc), err)
				return nil, nil
			}
			// pass any other error on
			return nil, err
		}
		return item, nil
	}
}

func shouldRetryError(ctx context.Context, d *QueryData, h *HydrateData, err error, retryConfig *RetryConfig) bool {
	log.Printf("[TRACE] shouldRetryError err: %v, retryConfig: %s", err, retryConfig.String())

	if retryConfig == nil {
		log.Printf("[TRACE] shouldRetryError nil retry config - return false")
		return false
	}

	if retryConfig.ShouldRetryError != nil {
		log.Printf("[TRACE] shouldRetryError - calling legacy ShouldRetryError")
		return retryConfig.ShouldRetryError(err)
	}

	if retryConfig.ShouldRetryErrorFunc != nil {
		log.Printf("[TRACE] shouldRetryError - calling ShouldRetryFunc")
		return retryConfig.ShouldRetryErrorFunc(ctx, d, h, err)
	}

	return false
}

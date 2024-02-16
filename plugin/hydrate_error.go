package plugin

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/sethvargo/go-retry"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryHydrate function invokes the hydrate function with retryable errors and retries the function until the maximum attempts before throwing error
func RetryHydrate(ctx context.Context, d *QueryData, hydrateData *HydrateData, hydrate HydrateFunc, retryConfig *RetryConfig) (hydrateResult interface{}, err error) {
	return retryNamedHydrate(ctx, d, hydrateData, newNamedHydrateFunc(hydrate), retryConfig)
}

func retryNamedHydrate(ctx context.Context, d *QueryData, hydrateData *HydrateData, hydrate namedHydrateFunc, retryConfig *RetryConfig) (hydrateResult interface{}, err error) {
	ctx, span := telemetry.StartSpan(ctx, d.Table.Plugin.Name, "RetryHydrate (%s)", d.Table.Name)
	span.SetAttributes(
		attribute.String("hydrate-func", hydrate.Name),
	)
	defer func() {
		if err != nil {
			span.SetAttributes(
				attribute.String("err", err.Error()),
			)
		}
		span.End()
	}()

	// Defaults
	maxAttempts := uint64(10) // default set to 10
	if retryConfig.MaxAttempts != 0 {
		maxAttempts = uint64(retryConfig.MaxAttempts)
	}

	// Create the backoff based on the given mode
	backoff, err := getBackoff(retryConfig)
	if err != nil {
		return nil, err
	}

	err = retry.Do(ctx, retry.WithMaxRetries(maxAttempts, backoff), func(ctx context.Context) error {
		hydrateResult, err = hydrate.Func(ctx, d, hydrateData)
		if err != nil {
			log.Printf("[TRACE] >>> error %s", err.Error())
			if shouldRetryError(ctx, d, hydrateData, err, retryConfig) {
				err = retry.RetryableError(err)
			}
		}
		return err
	})

	return hydrateResult, err
}

func getBackoff(retryConfig *RetryConfig) (retry.Backoff, error) {
	// Default set to Fibonacci
	backoffAlgorithm := "Fibonacci"
	var retryIntervalMs int64 = 100
	var cappedDurationMs, maxDurationMs int64

	// Check from config
	if retryConfig != nil {
		if retryConfig.BackoffAlgorithm != "" {
			backoffAlgorithm = retryConfig.BackoffAlgorithm
		}
		if retryConfig.RetryInterval != 0 {
			retryIntervalMs = retryConfig.RetryInterval
		}
		if retryConfig.CappedDuration != 0 {
			cappedDurationMs = retryConfig.CappedDuration
		}
		if retryConfig.MaxDuration != 0 {
			maxDurationMs = retryConfig.MaxDuration
		}
	}

	var backoff retry.Backoff
	var err error
	// convert retryIntervalMs into a duration
	retryInterval := time.Duration(retryIntervalMs) * time.Millisecond
	switch backoffAlgorithm {
	case "Fibonacci":
		backoff = retry.NewFibonacci(retryInterval)
	case "Exponential":
		backoff = retry.NewExponential(retryInterval)
	case "Constant":
		backoff = retry.NewConstant(retryInterval)
	}
	if err != nil {
		return nil, err
	}

	// Apply additional caps or limit
	if cappedDurationMs != 0 {
		backoff = retry.WithCappedDuration(time.Duration(cappedDurationMs)*time.Millisecond, backoff)
	}
	if maxDurationMs != 0 {
		backoff = retry.WithMaxDuration(time.Duration(maxDurationMs)*time.Second, backoff)
	}

	return backoff, nil
}

// WrapHydrate is a higher order function which returns a [HydrateFunc] that handles Ignorable errors.
func WrapHydrate(hydrate namedHydrateFunc, ignoreConfig *IgnoreConfig) namedHydrateFunc {
	res := hydrate.clone()

	res.Func = func(ctx context.Context, d *QueryData, h *HydrateData) (item interface{}, err error) {
		ctx, span := telemetry.StartSpan(ctx, d.Table.Plugin.Name, "hydrateWithIgnoreError (%s)", d.Table.Name)
		span.SetAttributes(
			attribute.String("hydrate-func", hydrate.Name),
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
				err = status.Error(codes.Internal, fmt.Sprintf("hydrate function %s failed with panic %v", hydrate.Name, r))
			}
		}()
		// call the underlying get function
		item, err = hydrate.Func(ctx, d, h)
		if err != nil {
			log.Printf("[TRACE] wrapped hydrate call %s returned error %v, ignore config %s\n", hydrate.Name, err, ignoreConfig.String())
			// see if the ignoreConfig defines a should ignore function
			if ignoreConfig.ShouldIgnoreError != nil && ignoreConfig.ShouldIgnoreError(err) {
				log.Printf("[TRACE] wrapped hydrate call %s returned error but we are ignoring it: %v", hydrate.Name, err)
				return nil, nil
			}
			if ignoreConfig.ShouldIgnoreErrorFunc != nil && ignoreConfig.ShouldIgnoreErrorFunc(ctx, d, h, err) {
				log.Printf("[TRACE] wrapped hydrate call %s returned error but we are ignoring it: %v", hydrate.Name, err)
				return nil, nil
			}
			// pass any other error on
			return nil, err
		}
		return item, nil
	}

	return res
}

func shouldRetryError(ctx context.Context, d *QueryData, h *HydrateData, err error, retryConfig *RetryConfig) bool {
	if retryConfig == nil {
		log.Printf("[WARN] nil retry config passed to shouldRetryError this is unexpected - returning false")
		return false
	}
	log.Printf("[TRACE] shouldRetryError err: %v, retryConfig: %s", err, retryConfig.String())

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

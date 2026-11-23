package plugin

// DefaultConcurrencyConfig is the type of [Plugin.DefaultConcurrency].
//
// Deprecated: The SDK no longer reads it, so setting it has no effect. To limit concurrency, set MaxConcurrency
// on a rate_limiter.Definition in [Plugin.RateLimiters]. It will be removed in v7.
type DefaultConcurrencyConfig struct {
	// sets how many HydrateFunc calls can run concurrently in total
	TotalMaxConcurrency int
	// sets the default for how many calls to each HydrateFunc can run concurrently
	DefaultMaxConcurrency int
}

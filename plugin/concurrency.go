package plugin

// Deprecated
type DefaultConcurrencyConfig struct {
	// sets how many HydrateFunc calls can run concurrently in total
	TotalMaxConcurrency int
	// sets the default for how many calls to each HydrateFunc can run concurrently
	DefaultMaxConcurrency int
}

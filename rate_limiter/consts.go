package rate_limiter

import "golang.org/x/time/rate"

const (
	RateLimiterKeyHydrate    = "hydrate"
	RateLimiterKeyConnection = "connection"

	// rates are per second
	DefaultPluginRate       rate.Limit = 5000
	DefaultPluginBurstSize             = 50
	DefaultHydrateRate                 = 15
	DefaultHydrateBurstSize            = 5
)

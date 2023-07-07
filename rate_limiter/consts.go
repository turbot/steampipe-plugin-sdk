package rate_limiter

import (
	"golang.org/x/time/rate"
	"os"
	"strconv"
)

const (
	RateLimiterKeyHydrate    = "hydrate"
	RateLimiterKeyConnection = "connection"

	// rates are per second
	DefaultPluginRate       rate.Limit = 5000
	DefaultPluginBurstSize             = 50
	DefaultHydrateRate                 = 15
	DefaultHydrateBurstSize            = 5

	EnvDefaultPluginRate       = "STEAMPIPE_DEFAULT_PLUGIN_RATE"
	EnvDefaultPluginBurstSize  = "STEAMPIPE_DEFAULT_PLUGIN_BURST"
	EnvDefaultHydrateRate      = "STEAMPIPE_DEFAULT_HYDRATE_RATE"
	EnvDefaultHydrateBurstSize = "STEAMPIPE_DEFAULT_HYDRATE_BURST"
)

func GetDefaultPluginRate() rate.Limit {
	if envStr, ok := os.LookupEnv(EnvDefaultPluginRate); ok {
		if r, err := strconv.Atoi(envStr); err == nil {
			return rate.Limit(r)
		}
	}
	return DefaultPluginRate
}

func GetDefaultPluginBurstSize() int {
	if envStr, ok := os.LookupEnv(EnvDefaultPluginBurstSize); ok {
		if b, err := strconv.Atoi(envStr); err == nil {
			return b
		}
	}
	return DefaultPluginBurstSize
}
func GetDefaultHydrateRate() rate.Limit {
	if envStr, ok := os.LookupEnv(EnvDefaultHydrateRate); ok {
		if r, err := strconv.Atoi(envStr); err == nil {
			return rate.Limit(r)
		}
	}
	return DefaultHydrateRate
}

func GetDefaultHydrateBurstSize() int {
	if envStr, ok := os.LookupEnv(EnvDefaultHydrateBurstSize); ok {
		if b, err := strconv.Atoi(envStr); err == nil {
			return b
		}
	}
	return DefaultHydrateBurstSize
}

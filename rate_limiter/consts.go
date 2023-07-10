package rate_limiter

import (
	"golang.org/x/time/rate"
	"os"
	"strconv"
	"strings"
)

const (
	RateLimiterKeyHydrate    = "hydrate"
	RateLimiterKeyConnection = "connection"

	defaultRateLimiterEnabled = false
	// rates are per second
	defaultPluginRate        rate.Limit = 5000
	defaultPluginBurstSize              = 50
	defaultHydrateRate                  = 15
	defaultHydrateBurstSize             = 5
	defaultMaxConcurrentRows            = 10

	envHydrateRateLimitEnabled = "STEAMPIPE_RATE_LIMIT_HYDRATE"
	envDefaultPluginRate       = "STEAMPIPE_DEFAULT_PLUGIN_RATE"
	envDefaultPluginBurstSize  = "STEAMPIPE_DEFAULT_PLUGIN_BURST"
	envDefaultHydrateRate      = "STEAMPIPE_DEFAULT_HYDRATE_RATE"
	envDefaultHydrateBurstSize = "STEAMPIPE_DEFAULT_HYDRATE_BURST"
	envMaxConcurrentRows       = "STEAMPIPE_MAX_CONCURRENT_ROWS"
)

func GetDefaultPluginRate() rate.Limit {
	if envStr, ok := os.LookupEnv(envDefaultPluginRate); ok {
		if r, err := strconv.Atoi(envStr); err == nil {
			return rate.Limit(r)
		}
	}
	return defaultPluginRate
}

func GetDefaultPluginBurstSize() int {
	if envStr, ok := os.LookupEnv(envDefaultPluginBurstSize); ok {
		if b, err := strconv.Atoi(envStr); err == nil {
			return b
		}
	}
	return defaultPluginBurstSize
}
func GetDefaultHydrateRate() rate.Limit {
	if envStr, ok := os.LookupEnv(envDefaultHydrateRate); ok {
		if r, err := strconv.Atoi(envStr); err == nil {
			return rate.Limit(r)
		}
	}
	return defaultHydrateRate
}

func GetDefaultHydrateBurstSize() int {
	if envStr, ok := os.LookupEnv(envDefaultHydrateBurstSize); ok {
		if b, err := strconv.Atoi(envStr); err == nil {
			return b
		}
	}
	return defaultHydrateBurstSize
}

func RateLimiterEnabled() bool {
	if envStr, ok := os.LookupEnv(envHydrateRateLimitEnabled); ok {
		return strings.ToLower(envStr) == "true" || strings.ToLower(envStr) == "on"
	}
	return defaultRateLimiterEnabled
}

func GetMaxConcurrentRows() int {
	if envStr, ok := os.LookupEnv(envMaxConcurrentRows); ok {
		if b, err := strconv.Atoi(envStr); err == nil {
			return b
		}
	}
	return defaultMaxConcurrentRows
}

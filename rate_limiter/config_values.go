package rate_limiter

import (
	"golang.org/x/time/rate"
	"os"
	"strconv"
	"strings"
)

const (
	// todo should these be more unique to avoid clash
	RateLimiterKeyHydrate    = "hydrate"
	RateLimiterKeyConnection = "connection"
	RateLimiterKeyPlugin     = "plugin"
	RateLimiterKeyTable      = "table"

	defaultRateLimiterEnabled = true
	// rates are per second
	defaultHydrateRate      = 150
	defaultHydrateBurstSize = 10

	defaultMaxConcurrentRows = 500

	envHydrateRateLimitEnabled = "STEAMPIPE_RATE_LIMIT_HYDRATE"
	envDefaultHydrateRate      = "STEAMPIPE_DEFAULT_HYDRATE_RATE"
	envDefaultHydrateBurstSize = "STEAMPIPE_DEFAULT_HYDRATE_BURST"
	envMaxConcurrentRows       = "STEAMPIPE_MAX_CONCURRENT_ROWS"
)

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

// DefaultConfig returns a config for a default rate limit config providing
// a single rate limiter for all calls to the plugin
func DefaultConfig() *Config {
	return &Config{
		Limiters: []*definition{
			{
				Limit:     GetDefaultHydrateRate(),
				BurstSize: GetDefaultHydrateBurstSize(),
				TagNames:  []string{RateLimiterKeyPlugin},
			}},
	}
}

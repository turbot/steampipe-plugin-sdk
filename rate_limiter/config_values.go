package rate_limiter

import (
	"os"
	"strconv"
)

const (
	// todo should these be more unique to avoid clash
	RateLimiterScopeHydrate    = "hydrate"
	RateLimiterScopeConnection = "connection"
	RateLimiterScopeTable      = "table"

	//defaultRateLimiterEnabled = false
	// rates are per second
	//defaultHydrateRate      = 50
	//defaultHydrateBurstSize = 5
	//
	defaultMaxConcurrentRows = 500

	//envRateLimitEnabled        = "STEAMPIPE_RATE_LIMIT_ENABLED"
	//envDefaultHydrateRate      = "STEAMPIPE_DEFAULT_HYDRATE_RATE"
	//envDefaultHydrateBurstSize = "STEAMPIPE_DEFAULT_HYDRATE_BURST"
	envMaxConcurrentRows = "STEAMPIPE_MAX_CONCURRENT_ROWS"
)

//func GetDefaultHydrateRate() rate.Limit {
//	if envStr, ok := os.LookupEnv(envDefaultHydrateRate); ok {
//		if r, err := strconv.Atoi(envStr); err == nil {
//			return rate.Limit(r)
//		}
//	}
//	return defaultHydrateRate
//}
//
//func GetDefaultHydrateBurstSize() int {
//	if envStr, ok := os.LookupEnv(envDefaultHydrateBurstSize); ok {
//		if b, err := strconv.Atoi(envStr); err == nil {
//			return b
//		}
//	}
//	return defaultHydrateBurstSize
//}
//
//func RateLimiterEnabled() bool {
//	if envStr, ok := os.LookupEnv(envRateLimitEnabled); ok {
//		return strings.ToLower(envStr) == "true" || strings.ToLower(envStr) == "on"
//	}
//	return defaultRateLimiterEnabled
//}

func GetMaxConcurrentRows() int {
	if envStr, ok := os.LookupEnv(envMaxConcurrentRows); ok {
		if b, err := strconv.Atoi(envStr); err == nil {
			return b
		}
	}
	return defaultMaxConcurrentRows
}

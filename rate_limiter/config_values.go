package rate_limiter

import (
	"os"
	"strconv"
)

const (
	RateLimiterScopeFunction   = "function_name"
	RateLimiterScopeConnection = "connection"
	RateLimiterScopeTable      = "table"

	defaultMaxConcurrentRows = 500
	envMaxConcurrentRows     = "STEAMPIPE_MAX_CONCURRENT_ROWS"
)

func GetMaxConcurrentRows() int {
	if envStr, ok := os.LookupEnv(envMaxConcurrentRows); ok {
		if b, err := strconv.Atoi(envStr); err == nil {
			return b
		}
	}
	return defaultMaxConcurrentRows
}

package rate_limiter

import (
	"os"
	"strconv"
)

const (
	// todo should these be more unique to avoid clash
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

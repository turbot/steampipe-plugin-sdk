package plugin

import (
	"math"
	"os"
	"strconv"
)

const (
	envMaxConcurrentConnection      = "STEAMPIPE_MAX_CONCURRENT_CONNECTIONS"
	envMaxConcurrentRow             = "STEAMPIPE_MAX_CONCURRENT_ROWS"
	envMaxMemoryMb                  = "STEAMPIPE_MAX_MEMORY_MB"
	envFreeMemInterval              = "STEAMPIPE_FREE_MEM_INTERVAL"
	defaultMaxConcurrentConnections = 5
	defaultMaxConcurrentRows        = 25
	defaultMaxMemoryMb              = math.MaxInt64 // no memory limit
	defaultFreeMemInterval          = 100
)

func getMaxConcurrentConnections() int {
	maxConcurrentConnections, _ := strconv.Atoi(os.Getenv(envMaxConcurrentConnection))
	if maxConcurrentConnections == 0 {
		maxConcurrentConnections = defaultMaxConcurrentConnections
	}
	return maxConcurrentConnections
}

func getMaxConcurrentRows() int {
	maxConcurrentRows, _ := strconv.Atoi(os.Getenv(envMaxConcurrentRow))
	if maxConcurrentRows == 0 {
		maxConcurrentRows = defaultMaxConcurrentRows
	}
	return maxConcurrentRows
}

func GetMaxMemoryBytes() int64 {
	maxMemoryMb, _ := strconv.Atoi(os.Getenv(envMaxMemoryMb))
	if maxMemoryMb == 0 {
		maxMemoryMb = defaultMaxMemoryMb
	}
	return int64(1024 * 1024 * maxMemoryMb)
}

func GetFreeMemInterval() int64 {
	freeMemInterval, _ := strconv.Atoi(os.Getenv(envFreeMemInterval))
	if freeMemInterval == 0 {
		freeMemInterval = defaultFreeMemInterval
	}
	return int64(freeMemInterval)
}

package plugin

import (
	"log"
	"math"
	"os"
	"strconv"
)

const (
	envMaxConcurrentConnection      = "STEAMPIPE_MAX_CONCURRENT_CONNECTIONS"
	envMaxMemoryMb                  = "STEAMPIPE_MAX_MEMORY_MB"
	envFreeMemInterval              = "STEAMPIPE_FREE_MEM_INTERVAL"
	defaultMaxConcurrentConnections = 25            // default to 25 concurrent connections
	defaultMaxMemoryMb              = math.MaxInt64 // default to no memory limit
	defaultFreeMemInterval          = 100           // default to freeing memory every 100 rows
)

func getMaxConcurrentConnections() int {
	maxConcurrentConnections, _ := strconv.Atoi(os.Getenv(envMaxConcurrentConnection))
	if maxConcurrentConnections == 0 {
		maxConcurrentConnections = defaultMaxConcurrentConnections
	}
	log.Printf("[INFO] Setting max concurrent connections to %d", maxConcurrentConnections)
	return maxConcurrentConnections
}

func GetMaxMemoryBytes() int64 {
	maxMemoryMb, _ := strconv.Atoi(os.Getenv(envMaxMemoryMb))
	if maxMemoryMb == 0 {
		log.Printf("[TRACE] No memory limit set")
		maxMemoryMb = defaultMaxMemoryMb
	} else {
		log.Printf("[TRACE] Setting max memory %dMb", maxMemoryMb)
	}
	return int64(1024 * 1024 * maxMemoryMb)
}

func GetFreeMemInterval() int64 {
	freeMemInterval := defaultFreeMemInterval
	intervalEnv, ok := os.LookupEnv(envFreeMemInterval)
	if ok {
		if parsedInterval, err := strconv.Atoi(intervalEnv); err == nil {
			freeMemInterval = parsedInterval
		}
	}
	log.Printf("[INFO] Setting free memory interval to %d rows", freeMemInterval)

	return int64(freeMemInterval)
}

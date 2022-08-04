package plugin

import (
	"log"
	"math"
	"os"
	"strconv"
)

const (
	envMaxConcurrentConnection      = "STEAMPIPE_MAX_CONCURRENT_CONNECTIONS"
	envMaxConcurrentRow             = "STEAMPIPE_MAX_CONCURRENT_ROWS"
	envMaxMemoryMb                  = "STEAMPIPE_MAX_MEMORY_MB"
	envFreeMemInterval              = "STEAMPIPE_FREE_MEM_INTERVAL"
	defaultMaxConcurrentConnections = 25            // default to 25 concurrent connections
	defaultMaxConcurrentRows        = math.MaxInt64 // default to no row limit
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

func getMaxConcurrentRows() int {
	maxConcurrentRows, _ := strconv.Atoi(os.Getenv(envMaxConcurrentRow))
	if maxConcurrentRows == 0 {
		log.Printf("[INFO] No max row concurrency set")
		maxConcurrentRows = defaultMaxConcurrentRows
	} else {
		log.Printf("[INFO] Setting max concurrent rows %d", maxConcurrentRows)
	}
	return maxConcurrentRows
}

func GetMaxMemoryBytes() int64 {
	maxMemoryMb, _ := strconv.Atoi(os.Getenv(envMaxMemoryMb))
	if maxMemoryMb == 0 {
		log.Printf("[INFO] No memory limit set")
		maxMemoryMb = defaultMaxMemoryMb
	} else {
		log.Printf("[INFO] Setting max memory %dMb", maxMemoryMb)
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

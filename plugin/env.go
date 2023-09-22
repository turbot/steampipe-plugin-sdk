package plugin

import (
	"fmt"
	"golang.org/x/exp/maps"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	envMaxConcurrentConnection      = "STEAMPIPE_MAX_CONCURRENT_CONNECTIONS"
	envFreeMemInterval              = "STEAMPIPE_FREE_MEM_INTERVAL"
	defaultMaxConcurrentConnections = 25  // default to 25 concurrent connections
	defaultFreeMemInterval          = 100 // default to freeing memory every 100 rows

	EnvDiagnosticsLevel       = "STEAMPIPE_DIAGNOSTIC_LEVEL"
	EnvLegacyDiagnosticsLevel = "STEAMPIPE_DIAGNOSTICS_LEVEL"
	DiagnosticsAll            = "ALL"
	DiagnosticsNone           = "NONE"
)

var ValidDiagnosticsLevels = map[string]struct{}{
	DiagnosticsAll:  {},
	DiagnosticsNone: {},
}

func getMaxConcurrentConnections() int {
	maxConcurrentConnections, _ := strconv.Atoi(os.Getenv(envMaxConcurrentConnection))
	if maxConcurrentConnections == 0 {
		maxConcurrentConnections = defaultMaxConcurrentConnections
	}
	log.Printf("[INFO] Setting max concurrent connections to %d", maxConcurrentConnections)
	return maxConcurrentConnections
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

func loadDiagnosticsEnvVar() string {
	// load both the legacy and current diagnostics env vars
	diagnostics := strings.ToUpper(os.Getenv(EnvLegacyDiagnosticsLevel))
	if newDiagnostics, isSet := os.LookupEnv(EnvDiagnosticsLevel); isSet {
		diagnostics = strings.ToUpper(newDiagnostics)
	}
	return diagnostics
}

func ValidateDiagnosticsEnvVar() error {
	diagnostics := loadDiagnosticsEnvVar()
	if diagnostics == "" {
		return nil
	}
	if _, isValid := ValidDiagnosticsLevels[strings.ToUpper(diagnostics)]; !isValid {
		return fmt.Errorf(`invalid value of '%s' (%s), must be one of: %s`, EnvDiagnosticsLevel, diagnostics, strings.Join(maps.Keys(ValidDiagnosticsLevels), ", "))
	}
	return nil
}

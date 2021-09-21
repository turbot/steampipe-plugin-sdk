package logging

import (
	"github.com/hashicorp/go-hclog"

	"os"
)

// NewLogger creates a hclog logger with the level specified by the SP_LOG env var
func NewLogger(options *hclog.LoggerOptions) hclog.Logger {
	if options.Level == hclog.NoLevel {
		level := LogLevel()
		if options == nil {
			options = &hclog.LoggerOptions{}
		}
		options.Level = hclog.LevelFromString(level)
	}
	if options.Output == nil {
		options.Output = os.Stderr
	}
	return hclog.New(options)
}

func LogLevel() string {
	level, ok := os.LookupEnv(LogLevelEnvVar)
	if !ok {
		level, ok = os.LookupEnv(LegacyLogLevelEnvVar)
		if !ok {
			level = defaultLogLevel
		}
	}
	return level
}

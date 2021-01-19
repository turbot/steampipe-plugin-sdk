package logging

import (
	"github.com/hashicorp/go-hclog"

	"os"
)

// NewLogger :: create a hclog logger with the level specified by the SP_LOG env var
func NewLogger(options *hclog.LoggerOptions) hclog.Logger {
	if options.Level == hclog.NoLevel {
		level, ok := os.LookupEnv(LogLevelEnvVar)
		if !ok {
			level = defaultLogLevel
		}
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

package logging

const (
	// LogLevelEnvVar :: environment variable defining the steampipe log level
	LegacyLogLevelEnvVar = "SP_LOG"
	LegacyProfileEnvVar  = "SP_PROFILE"
	LogLevelEnvVar       = "STEAMPIPE_LOG"
	ProfileEnvVar        = "STEAMPIPE_PROFILE"
	defaultLogLevel      = "WARN"
)

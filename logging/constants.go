package logging

// environment variable defining the steampipe log level

const (
	LegacyProfileEnvVar = "SP_PROFILE"
	LogLevelEnvVar      = "STEAMPIPE_LOG_LEVEL"
	ProfileEnvVar       = "STEAMPIPE_PROFILE"
	defaultLogLevel     = "WARN"
)

var LegacyLogLevelEnvVars = []string{"SP_LOG", "STEAMPIPE_LOG"}

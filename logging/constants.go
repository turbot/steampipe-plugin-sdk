package logging

// environment variable defining the steampipe log level

const (
	EnvLogLevel     = "STEAMPIPE_LOG_LEVEL"
	EnvProfile      = "STEAMPIPE_PROFILE"
	defaultLogLevel = "WARN"
)

var LegacyLogLevelEnvVars = []string{"SP_LOG", "STEAMPIPE_LOG"}

var newLine = []byte("\n")
var escapedNewLine = []byte("\\n")

package instrument

const EnvTelemetry = "STEAMPIPE_TELEMETRY"

// constants for telemetry config flag
const (
	TelemetryNone = "none"
	TelemetryInfo = "info"
)

var TelemetryLevels = []string{TelemetryNone, TelemetryInfo}

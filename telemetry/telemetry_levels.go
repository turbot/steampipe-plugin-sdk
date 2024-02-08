package telemetry

const EnvOtelInsecure = "STEAMPIPE_OTEL_INSECURE"
const EnvOtelLevel = "STEAMPIPE_OTEL_LEVEL"
const EnvOtelEndpoint = "OTEL_EXPORTER_OTLP_ENDPOINT"

// constants for telemetry config flag
const (
	OtelNone    = "none"
	OtelAll     = "all"
	OtelTrace   = "trace"
	OtelMetrics = "metrics"
)

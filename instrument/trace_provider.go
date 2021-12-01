package instrument

import (
	"context"
	"fmt"

	"github.com/turbot/steampipe/constants"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	TRACER_NAME     = "fdw"
	TRACER_ENDPOINT = "http://localhost:55681/api/traces"
)

func NeedsInit() {
	return (otel.GetTracerProvider() == nil)
}

// tracerProvider returns an OpenTelemetry TracerProvider configured to use
// the Jaeger exporter that will send spans to the provided url. The returned
// TracerProvider will also use a Resource configured with all the information
// about the application.
func InitTracing(componentName string, componentVersion string) error {
	exporter, err := getJaegerExporter()
	if err != nil {
		return err
	}
	tp := tracesdk.NewTracerProvider(
		// Always be sure to batch in production.
		tracesdk.WithBatcher(exporter),
		// Record information about this application in a Resource.
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(componentName),
			semconv.ServiceVersionKey.String(componentVersion),
		)),
	)

	otel.SetTracerProvider(tp)

	return nil
}

func getHttpTracerExporter() (*otlptrace.Exporter, error) {
	client := otlptracehttp.NewClient(
		otlptracehttp.WithEndpoint("localhost:55681"),
		otlptracehttp.WithInsecure(),
	)
	return otlptrace.New(context.Background(), client)
}

func getStdOutExporter() (*stdouttrace.Exporter, error) {
	return stdouttrace.New()
}

func getJaegerExporter() (*jaeger.Exporter, error) {
	return jaeger.New(
		jaeger.WithCollectorEndpoint(
			jaeger.WithEndpoint("http://localhost:14268/api/traces")))
}

func ShutdownTracing() {
	defer func() {
		// artificially prevent a panic in this fn
		recover()
	}()
	otel.GetTracerProvider().(*tracesdk.TracerProvider).ForceFlush(context.Background())
	otel.GetTracerProvider().(*tracesdk.TracerProvider).Shutdown(context.Background())
}

func FlushTraces() {
	defer func() {
		// artificially prevent a panic in this fn
		recover()
	}()
	otel.GetTracerProvider().(*tracesdk.TracerProvider).ForceFlush(context.Background())
}

func GetTracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer(constants.AppName)
}

func StartRootSpan(id string) (context.Context, trace.Span) {
	tr := GetTracer()
	id = "callId"
	traceContext, span := tr.Start(context.Background(), id)
	span.SetAttributes(attribute.Key(id).String(id))

	return traceContext, span
}

func StartSpan(baseCtx context.Context, format string, args ...interface{}) (context.Context, trace.Span) {
	tr := GetTracer()
	return tr.Start(baseCtx, fmt.Sprintf(format, args))
}

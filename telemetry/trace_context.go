package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func GetTracer(service string) trace.Tracer {
	return otel.GetTracerProvider().Tracer(service)
}

func StartSpan(baseCtx context.Context, service string, format string, args ...interface{}) (context.Context, trace.Span) {
	tr := GetTracer(service)
	return tr.Start(baseCtx, fmt.Sprintf(format, args...))
}

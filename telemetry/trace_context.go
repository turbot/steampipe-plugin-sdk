package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// TraceCtx is a struct which contains a span and the associated context
// This is used by the FDW for persisting [`span`,`context`] tuples across PostreSQL callbacks
type TraceCtx struct {
	Ctx  context.Context
	Span trace.Span
}

func GetTracer(service string) trace.Tracer {
	return otel.GetTracerProvider().Tracer(service)
}

func StartSpan(baseCtx context.Context, service string, format string, args ...interface{}) (context.Context, trace.Span) {
	tr := GetTracer(service)
	return tr.Start(baseCtx, fmt.Sprintf(format, args...))
}

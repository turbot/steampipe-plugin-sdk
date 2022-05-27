package instrument

import (
	"context"
	"fmt"

	"github.com/turbot/steampipe/constants"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// TraceCtx is a struct which contains a span and the associated context
type TraceCtx struct {
	Ctx  context.Context
	Span trace.Span
}

func GetTracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer(constants.AppName)
}

//func StartRootSpan(id string) *TraceCtx {
//	tr := GetTracer()
//	traceContext, span := tr.Start(context.Background(), id)
//	span.SetAttributes(attribute.Key(id).String(id))
//
//	return &TraceCtx{Ctx: traceContext, Span: span}
//}

func StartSpan(baseCtx context.Context, format string, args ...interface{}) (context.Context, trace.Span) {
	tr := GetTracer()
	return tr.Start(baseCtx, fmt.Sprintf(format, args...))
}

// TODO doesn't seem to be needed
//func FlushTraces() {
//	defer func() {
//		// artificially prevent a panic in this fn
//		recover()
//	}()
//	otel.GetTracerProvider().(*tracesdk.TracerProvider).ForceFlush(context.Background())
//}

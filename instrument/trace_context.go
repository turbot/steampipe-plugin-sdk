package instrument

import (
	"context"
	"encoding/json"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"go.opentelemetry.io/otel/propagation"
)

var propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

func CreateCarrierFromContext(ctx context.Context) *proto.TraceContext {
	// Inject trace context information from context onto the carrier
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)

	// Transform carrier data to be sent back as a string value
	carrierData, err := json.Marshal(carrier)
	if err != nil {
		return &proto.TraceContext{Value: ""}
	}
	return &proto.TraceContext{Value: string(carrierData)}
}

func ExtractContextFromCarrier(ctx context.Context, traceCtx *proto.TraceContext) context.Context {
	if traceCtx == nil || len(traceCtx.Value) == 0 {
		return ctx
	}

	carrier := propagation.MapCarrier{}

	// Convert raw trace context data into MapCarrier
	err := json.Unmarshal([]byte(traceCtx.Value), &carrier)
	if err != nil {
		return ctx
	}

	// Frame a new context with extracted trace context information from carrier
	return propagator.Extract(ctx, carrier)
}
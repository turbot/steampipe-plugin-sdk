package grpc

import (
	"context"
	"encoding/json"
	"log"

	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"go.opentelemetry.io/otel/propagation"
)

func CreateCarrierFromContext(ctx context.Context) *proto.TraceContext {
	// Inject trace context information from context onto the carrier
	carrier := propagation.MapCarrier{}
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
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

	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	carrier := propagation.MapCarrier{}

	// Convert raw trace context data into MapCarrier
	err := json.Unmarshal([]byte(traceCtx.Value), &carrier)
	if err != nil {
		log.Printf("[WARN] ExtractContextFromCarrier unmarshal error: %s", err.Error())
		return ctx
	}

	// Frame a new context with extracted trace context information from carrier
	return propagator.Extract(ctx, carrier)
}

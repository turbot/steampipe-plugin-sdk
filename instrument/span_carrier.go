package instrument

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

func CreateCarrierFromContext(ctx context.Context) *proto.Tracer {
	return &proto.Tracer{Value: ""}
}

func ExtractContextFromCarrier(carrier *proto.Tracer) context.Context {
	if carrier == nil || len(carrier.Value) == 0 {
		return context.Background()
	}
	return context.TODO()
}

package instrument

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

func CreateCarrierFromContext(ctx context.Context) *proto.SpanCarrier {
	return &proto.SpanCarrier{Value: map[string]string{}}
}

func ExtractContextFromCarrier(carrier *proto.SpanCarrier) context.Context {
	if carrier == nil || len(carrier.Value) == 0 {
		return context.Background()
	}
	return context.TODO()
}

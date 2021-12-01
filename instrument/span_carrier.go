package instrument

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

func CreateCarrierFromContext(ctx context.Context) *proto.SpanCarrier {
	return &proto.SpanCarrier{Value: map[string]string{}}
}

func ExtractContextFromCarrier(carrier *proto.SpanCarrier) context.Context {
	return context.TODO()
}

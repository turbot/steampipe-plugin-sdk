package instrument

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

func CreateCarrierFromContext(ctx context.Context) *proto.TraceContext {
	return &proto.TraceContext{Value: ""}
}

func ExtractContextFromCarrier(traceCtx *proto.TraceContext) context.Context {
	if traceCtx == nil || len(traceCtx.Value) == 0 {
		return context.Background()
	}
	return context.TODO()
}

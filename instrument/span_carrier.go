package instrument

import (
	"context"
)

type SpanCarrier map[string]string

func CreateCarrierFromContext(ctx context.Context) SpanCarrier {
	return map[string]string{}
}

func ExtractContextFromCarrier(carrier SpanCarrier) context.Context {
	return context.TODO()
}

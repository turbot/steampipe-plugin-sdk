package proto

import (
	"github.com/golang/protobuf/ptypes/timestamp"
	typehelpers "github.com/turbot/go-kit/types"
)

func (x *Quals) Append(q *Qual) {
	x.Quals = append(x.Quals, q)
}

// NewQualValue creates a QualValue object from a raw value
func NewQualValue(value interface{}) *QualValue {
	// TODO handle lists separately

	switch v := value.(type) {
	case int:
		return &QualValue{Value: &QualValue_Int64Value{Int64Value: int64(v)}}
	case int32:
		return &QualValue{Value: &QualValue_Int64Value{Int64Value: int64(v)}}
	case int64:
		return &QualValue{Value: &QualValue_Int64Value{Int64Value: v}}
	case float32:
		return &QualValue{Value: &QualValue_DoubleValue{DoubleValue: float64(v)}}
	case float64:
		return &QualValue{Value: &QualValue_DoubleValue{DoubleValue: v}}
	case *timestamp.Timestamp:
		return &QualValue{Value: &QualValue_TimestampValue{TimestampValue: v}}
	case bool:
		return &QualValue{Value: &QualValue_BoolValue{BoolValue: v}}
	default:
		return &QualValue{Value: &QualValue_StringValue{StringValue: typehelpers.ToString(v)}}
	}
}

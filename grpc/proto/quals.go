package proto

import (
	"fmt"

	"github.com/golang/protobuf/ptypes/timestamp"
	typehelpers "github.com/turbot/go-kit/types"
)

func (x *Quals) Append(q *Qual) {
	x.Quals = append(x.Quals, q)
}

func (x *Quals) IsASubsetOf(other *Quals) bool {
	// all quals in x must exist in other
	for _, q := range x.Quals {
		if !other.Contains(q) {
			return false
		}
	}
	return true
}

func (x *Quals) Contains(otherQual *Qual) bool {
	for _, q := range x.Quals {
		if q.Equals(otherQual) {
			return true
		}
	}
	return false
}

func (x *Qual) Equals(other *Qual) bool {
	return fmt.Sprintf("%v", x.Value.Value) == fmt.Sprintf("%v", other.Value.Value) &&
		x.FieldName == other.FieldName &&
		x.Operator == other.Operator
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

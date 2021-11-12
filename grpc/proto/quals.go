package proto

import (
	"log"

	"github.com/golang/protobuf/ptypes/timestamp"
	typehelpers "github.com/turbot/go-kit/types"
)

func (x *Quals) Append(q *Qual) {
	x.Quals = append(x.Quals, q)
}

func (x *Quals) IsASubsetOf(other *Quals) bool {
	log.Printf("[INFO] IsASubsetOf")
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
		log.Printf("[INFO] Contains me %+v, other %+v", q, otherQual)
		if q.Equals(otherQual) {
			return true
		}
		log.Printf("[INFO] !=")
	}
	return false
}

func (x *Qual) Equals(other *Qual) bool {
	log.Printf("[INFO]  me %s, other %s", x.String(), other.String())
	return x.String() == other.String()
}

func (x *Qual) IsASubsetOf(other *Qual) bool {
	operator, ok := x.Operator.(*Qual_StringValue)
	if !ok {
		return false
	}
	otherOperator, ok := x.Operator.(*Qual_StringValue)
	if !ok {
		return false
	}

	// if operators are different then we are not a subset
	if operator != otherOperator {
		return false
	}
	// if operators are both equals then the quals must be equal
	if operator.StringValue == "=" {
		return x.Equals(other)
	}

	switch x.Value.Value.(type) {
	//case *QualValue_StringValue:
	case *QualValue_Int64Value:
	case *QualValue_DoubleValue:
		// whiuch operatros can ewe handle for this subset check
		operators := []string{""}
		if
	//case *QualValue_BoolValue:
	//case *QualValue_InetValue	:
	//case *QualValue_JsonbValue:
	case *QualValue_TimestampValue:
	case *QualValue_ListValue:

	}
	log.Printf("[INFO]  me %s, other %s", x.String(), other.String())
	return x.String() == other.String()
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

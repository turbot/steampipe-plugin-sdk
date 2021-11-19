package proto

import (
	"log"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	typehelpers "github.com/turbot/go-kit/types"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (x *Quals) Append(q *Qual) {
	x.Quals = append(x.Quals, q)
}

// IsASubsetOf determines whether we are a subset of other Quals
func (x *Quals) IsASubsetOf(other *Quals) bool {
	log.Printf("[TRACE] IsASubsetOf me %+v other %+v", x, other)
	// all quals in x must be a subset of all the quals in other
	for _, qual := range x.Quals {
		if !other.QualIsASubset(qual) {
			return false
		}
	}
	return true
}

// QualIsASubset determines whether otherQual is a subset of all our quals
func (x *Quals) QualIsASubset(otherQual *Qual) bool {
	log.Printf("[TRACE] QualIsASubset my quals %+v, other %+v", x, otherQual)
	for _, q := range x.Quals {
		if !otherQual.IsASubsetOf(q) {
			log.Printf("[TRACE] otherQual %+v is NOT a subset of %+v", otherQual, q)
			return false
		}
		log.Printf("[TRACE] otherQual %+v IS a subset of %+v", otherQual, q)
	}

	log.Printf("[WARN] QualIsASubset returning true")
	return true
}

func (x *Qual) Equals(other *Qual) bool {
	log.Printf("[TRACE] me %s, other %s", x.String(), other.String())
	return x.String() == other.String()
}

func (x *Qual) IsASubsetOf(other *Qual) bool {
	operator, ok := x.Operator.(*Qual_StringValue)
	if !ok {
		log.Printf("[TRACE] IsASubsetOf my operator is not a string - returning false")
		return false
	}
	otherOperator, ok := other.Operator.(*Qual_StringValue)
	if !ok {
		log.Printf("[TRACE] IsASubsetOf other operator is not a string - returning false")
		return false
	}
	if x.FieldName != other.FieldName {
		log.Printf("[TRACE] IsASubsetOf field names different - returning false")
		return false
	}

	switch value := x.Value.Value.(type) {
	case *QualValue_StringValue:
		otherValue, ok := other.Value.Value.(*QualValue_StringValue)
		if !ok {
			return false
		}
		return stringOperatorIsASubset(operator.StringValue, value.StringValue, otherOperator.StringValue, otherValue.StringValue)
	case *QualValue_Int64Value:
		otherValue, ok := other.Value.Value.(*QualValue_Int64Value)
		if !ok {
			return false
		}
		return intOperatorIsASubset(operator.StringValue, value.Int64Value, otherOperator.StringValue, otherValue.Int64Value)
	case *QualValue_DoubleValue:
		otherVal, ok := other.Value.Value.(*QualValue_DoubleValue)
		if !ok {
			return false
		}
		return doubleOperatorIsASubset(operator.StringValue, value.DoubleValue, otherOperator.StringValue, otherVal.DoubleValue)
	case *QualValue_TimestampValue:
		otherVal, ok := other.Value.Value.(*QualValue_TimestampValue)
		if !ok {
			return false
		}
		return timeOperatorIsASubset(operator.StringValue, value.TimestampValue, otherOperator.StringValue, otherVal.TimestampValue)
	case *QualValue_BoolValue:
		otherVal, ok := other.Value.Value.(*QualValue_BoolValue)
		if !ok {
			return false
		}
		return boolOperatorIsASubset(operator.StringValue, value.BoolValue, otherOperator.StringValue, otherVal.BoolValue)

	case *QualValue_InetValue:
		otherVal, ok := other.Value.Value.(*QualValue_InetValue)
		if !ok {
			return false
		}
		return inetOperatorIsASubset(operator.StringValue, value.InetValue.Addr, otherOperator.StringValue, otherVal.InetValue.Addr)

	case *QualValue_ListValue:
		otherVal, ok := other.Value.Value.(*QualValue_ListValue)
		if !ok {
			return false
		}
		return listOperatorIsASubset(operator.StringValue, value.ListValue.Values, otherOperator.StringValue, otherVal.ListValue.Values)
	}

	log.Printf("[TRACE] IsASubsetOf no supported types = returning false")
	return false
}

func stringOperatorIsASubset(operator string, value string, otherOperator string, otherValue string) bool {
	switch operator {
	case "=":
		switch otherOperator {
		case "=":
			return value == otherValue
		default:
			return false
		}
	case "!=":
		switch otherOperator {
		case "!=":
			return value == otherValue
		default:
			return false
		}
	}

	return false
}

func listOperatorIsASubset(operator string, value []*QualValue, otherOperator string, otherValue []*QualValue) bool {
	// only support equals
	switch operator {
	case "=":
		switch otherOperator {
		case "=":
			// all elements in value must be contained in otherValue
			for _, e := range value {
				if !qualValueListContains(otherValue, e) {
					return false
				}
			}
			return true
		default:
			return false
		}
	}

	return false
}

func qualValueListContains(list []*QualValue, otherElement *QualValue) bool {
	for _, e := range list {
		if e.String() == otherElement.String() {
			return true
		}
	}
	return false
}

func inetOperatorIsASubset(operator string, value string, otherOperator string, otherValue string) bool {
	switch operator {
	case "=":
		switch otherOperator {
		case "=":
			return value == otherValue
		default:
			return false
		}
	case "!=":
		switch otherOperator {
		case "!=":
			return value == otherValue
		default:
			return false
		}
	}

	return false
}
func boolOperatorIsASubset(operator string, value bool, otherOperator string, otherValue bool) bool {
	switch operator {
	case "=":
		switch otherOperator {
		case "=":
			return value == otherValue
		default:
			return false
		}
	case "!=":
		switch otherOperator {
		case "!=":
			return value == otherValue
		default:
			return false
		}
	}

	return false
}

// is operator and value a subset of otherOperator and otherValue
func doubleOperatorIsASubset(operator string, value float64, otherOperator string, otherValue float64) bool {
	switch operator {
	case "=":
		switch otherOperator {
		case "=":
			return value == otherValue
		case "<":
			// value = 9.9, otherValue < 10 - subset
			// value = 10, otherValue < 10 - NOT subset
			return value < otherValue
		case "<=":
			// value = 9.9, otherValue <= 10 - subset
			// value = 10, otherValue <= 10 - subset
			// value = 10.1, otherValue <= 10 - NOT subset
			return value <= otherValue
		case ">":
			// value = 10, otherValue > 10 - NOT subset
			// value = 10.1, otherValue > 10 -  subset
			return value > otherValue
		case ">=":
			// value = 9.9, otherValue >= 10 - NOT subset
			// value = 10, otherValue >= 10 - subset
			// value = 10.1, otherValue >= 10 - subset
			return value >= otherValue
		default:
			return false
		}
	case "!=":
		switch otherOperator {
		case "!=":
			return value == otherValue
		default:
			return false
		}

	case "<":
		switch otherOperator {
		case "<":
			// value < 9.9, otherValue < 10 - subset
			// value < 10, otherValue < 10 - subset
			// value < 10.1, otherValue < 10 - NOT subset
			return value <= otherValue
		case "<=":
			// value < 9.9, otherValue <= 10 - subset
			// value < 10, otherValue <= 10 - subset
			// value < 10.1, otherValue <= 10 - NOT subset
			return value <= otherValue
		default:
			return false
		}
	case "<=":
		switch otherOperator {
		case "<":
			// value <= 9.9, otherValue < 10 - subset
			// value <= 10, otherValue < 10 - NOT subset
			return value < otherValue
		case "<=":
			// value <= 9.9, otherValue <= 10 - subset
			// value <= 10, otherValue <= 10 - subset
			// value <= 10.1, otherValue <= 10 - NOT subset
			return value <= otherValue
		default:
			return false
		}
	case ">":
		switch otherOperator {
		case ">":
			// value > 9.9, otherValue > 10 - NOT subset
			// value > 10, otherValue > 10 - subset
			// value > 10.1, otherValue > 10 - subset
			return value >= otherValue
		case ">=":
			// value > 9.9, otherValue >= 10 - NOT subset
			// value > 10, otherValue >= 10 - subset
			return value >= otherValue
		default:
			return false
		}
	case ">=":
		switch otherOperator {
		case ">":
			// value >= 10, otherValue > 10 - NOT subset
			// value >= 10.1, otherValue > 10 - subset
			// value >= 10.2, otherValue > 10 - subset
			return value > otherValue
		case ">=":
			// value >= 9.9, otherValue >= 10 - NOT subset
			// value >= 10, otherValue >= 10 - subset
			// value >= 10.1, otherValue >= 10 - subset
			return value >= otherValue
		default:
			return false
		}
	}

	return false
}

func intOperatorIsASubset(operator string, value int64, otherOperator string, otherValue int64) bool {
	switch operator {
	case "=":
		switch otherOperator {
		case "=":
			return value == otherValue
		case "<":
			// value = 9, otherValue < 10 - subset
			// value = 10, otherValue < 10 - NOT subset
			return value < otherValue
		case "<=":
			// value = 9, otherValue <= 10 - subset
			// value = 10, otherValue <= 10 - subset
			// value = 11, otherValue <= 10 - NOT subset
			return value <= otherValue
		case ">":
			// value = 10, otherValue > 10 - NOT subset
			// value = 11, otherValue > 10 -  subset
			return value > otherValue
		case ">=":
			// value = 9, otherValue >= 10 - NOT subset
			// value = 10, otherValue >= 10 - subset
			// value = 11, otherValue >= 10 - subset
			return value >= otherValue
		default:
			return false
		}
	case "!=":
		switch otherOperator {
		case "!=":
			return value == otherValue
		default:
			return false
		}
	case "<":
		switch otherOperator {
		case "<":
			// value < 9, otherValue < 10 - subset
			// value < 10, otherValue < 10 - subset
			// value < 11, otherValue < 10 - NOT subset
			return value <= otherValue
		case "<=":
			// value < 10, otherValue <= 10 - subset
			// value < 11, otherValue <= 10 - subset
			// value < 12, otherValue <= 10 - NOT subset
			return value-1 <= otherValue
		default:
			return false
		}
	case "<=":
		switch otherOperator {
		case "<":
			// value <= 8, otherValue < 10 - subset
			// value <= 9, otherValue < 10 - subset
			// value <= 10, otherValue < 10 - NOT subset
			return value < otherValue
		case "<=":
			// value <= 9, otherValue <= 10 - subset
			// value <= 10, otherValue <= 10 - subset
			// value <= 11, otherValue <= 10 - NOT subset
			return value <= otherValue
		default:
			return false
		}
	case ">":
		switch otherOperator {
		case ">":
			// value > 9, otherValue > 10 - NOT subset
			// value > 10, otherValue > 10 - subset
			// value > 11, otherValue > 10 - subset
			return value >= otherValue
		case ">=":
			// value > 8, otherValue >= 10 - NOT subset
			// value > 9, otherValue >= 10 - subset
			// value > 10, otherValue >= 10 - subset
			return value+1 >= otherValue
		default:
			return false
		}
	case ">=":
		switch otherOperator {
		case ">":
			// value >= 10, otherValue > 10 - NOT subset
			// value >= 11, otherValue > 10 - subset
			// value >= 12, otherValue > 10 - subset
			return value > otherValue
		case ">=":
			// value >= 9, otherValue >= 10 - NOT subset
			// value >= 10, otherValue >= 10 - subset
			// value >= 11, otherValue >= 10 - subset
			return value >= otherValue
		default:
			return false
		}
	}

	return false

}

func timeOperatorIsASubset(operator string, value *timestamppb.Timestamp, otherOperator string, otherValue *timestamppb.Timestamp) bool {
	timeVal := time.Unix(value.Seconds, int64(value.Nanos))
	otherTimeVal := time.Unix(otherValue.Seconds, int64(otherValue.Nanos))
	switch operator {
	case "=":
		switch otherOperator {
		case "=":
			return timeVal == otherTimeVal
		case "<":
			return timeVal.Before(otherTimeVal)
		case "<=":
			return timeVal.Before(otherTimeVal) || timeVal == otherTimeVal
		case ">":
			return timeVal.After(otherTimeVal)
		case ">=":
			return timeVal.After(otherTimeVal) || timeVal == otherTimeVal
		default:
			return false
		}
	case "!=":
		switch otherOperator {
		case "!=":
			return timeVal == otherTimeVal
		default:
			return false
		}
	case "<":
		switch otherOperator {
		case "<", "<=":
			return timeVal.Before(otherTimeVal) || timeVal == otherTimeVal
		default:
			return false
		}
	case "<=":
		switch otherOperator {
		case "<":
			return timeVal.Before(otherTimeVal)
		case "<=":
			return timeVal.Before(otherTimeVal) || timeVal == otherTimeVal
		default:
			return false
		}
	case ">":
		switch otherOperator {
		case ">", ">=":
			return timeVal.After(otherTimeVal) || timeVal == otherTimeVal
		default:
			return false
		}
	case ">=":
		switch otherOperator {
		case ">":
			return timeVal.After(otherTimeVal)
		case ">=":
			return timeVal.After(otherTimeVal) || timeVal == otherTimeVal
		default:
			return false
		}
	}

	return false

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

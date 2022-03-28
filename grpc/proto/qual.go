package proto

import "log"

func (x *Qual) Equals(other *Qual) bool {
	//log.Printf("[TRACE] me %s, other %s", x.String(), other.String())
	return x.String() == other.String()
}

func (x *Qual) IsASubsetOf(other *Qual) bool {
	//log.Printf("[TRACE] IsASubsetOf me %+v, other %+v", x, other)
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
	if x.Value == nil {
		log.Printf("[TRACE] IsASubsetOf Value nil - returning false")
		return false
	}
	// check the fields are the same
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

package grpc

import (
	"fmt"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

const (
	IPv4 = "IPv4"
	IPv6 = "IPv6"
)

func QualMapToString(qualMap map[string]*proto.Quals) interface{} {
	str := "\n\t"
	for _, quals := range qualMap {
		qualString := ""
		for _, q := range quals.GetQuals() {
			qualString += QualToString(q)
		}
		str += qualString
	}
	return str
}

func QualToString(q *proto.Qual) string {
	return fmt.Sprintf("Column: %s, Operator: '%s', Value: '%v'\n\t", q.FieldName, q.GetStringValue(), getQualValue(q.Value))
}

func QualEquals(left *proto.Qual, right *proto.Qual) bool {
	return QualToString(left) == QualToString(right)
}

func getQualValue(v *proto.QualValue) interface{} {
	var qv interface{}
	switch v := v.GetValue().(type) {
	case *proto.QualValue_InetValue:
		qv = v.InetValue.Cidr
	case *proto.QualValue_JsonbValue:
		qv = v.JsonbValue
	case *proto.QualValue_StringValue:
		qv = v.StringValue
	case *proto.QualValue_Int64Value:
		qv = v.Int64Value
	case *proto.QualValue_DoubleValue:
		qv = v.DoubleValue
	case *proto.QualValue_BoolValue:
		qv = v.BoolValue
	case *proto.QualValue_ListValue:
		var values []interface{}
		for _, l := range v.ListValue.Values {
			values = append(values, getQualValue(l))
		}
		qv = values
	}
	return qv
}

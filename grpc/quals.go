package grpc

import (
	"fmt"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

const (
	IPv4 = "IPv4"
	IPv6 = "IPv6"
)

func QualMapToString(qualMap map[string]*proto.DbQuals) interface{} {
	divider := "----------------------------------------------------------------\n"
	str := fmt.Sprintf("\n%s", divider)
	for _, quals := range qualMap {
		qualString := ""
		for _, q := range quals.GetQuals() {
			qualString += QualToString(q)
		}
		str += qualString
	}
	str += divider
	return str
}

func QualToString(d *proto.DbQual) string {
	q := d.GetQual()
	if q != nil {
		return fmt.Sprintf("\tColumn: %s, Operator: '%s', Value: '%v'\n", q.FieldName, q.Operator, GetQualValue(q.Value))
	}
	// TODO BOOL QUAL TO STRING
	return "BOOL QUAL"
}

func GetQualValue(v *proto.QualValue) interface{} {
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
	case *proto.QualValue_TimestampValue:
		ts := v.TimestampValue
		qv = time.Unix(ts.Seconds, int64(ts.Nanos))
	case *proto.QualValue_ListValue:
		var values []interface{}
		for _, l := range v.ListValue.Values {
			values = append(values, GetQualValue(l))
		}
		qv = values
	}
	return qv
}

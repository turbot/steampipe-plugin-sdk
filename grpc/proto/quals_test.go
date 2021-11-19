package proto

import (
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// is q1 a subset of q2
type isSubsetTest struct {
	q1       *Qual
	q2       *Qual
	expected bool
}

var now = time.Now()
var testCasesIsSubset = map[string]isSubsetTest{
	"both = same string": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}},
		true,
	},
	"both = same int64": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_Int64Value{Int64Value: 100}}},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_Int64Value{Int64Value: 100}}},
		true,
	},
	"both = same double": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_DoubleValue{DoubleValue: 100}}},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_DoubleValue{DoubleValue: 100}}},
		true,
	},
	"both = same inet": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: ToInet("192.168.0.1")}},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: ToInet("192.168.0.1")}},
		true,
	},
	"both = same bool": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_BoolValue{BoolValue: true}}},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_BoolValue{BoolValue: true}}},
		true,
	},
	"both = same jsonb": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_JsonbValue{JsonbValue: "10"}}},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_JsonbValue{JsonbValue: "10"}}},
		true,
	},
	"both = same timestamp": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: getTimestampValue(now)},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: getTimestampValue(now)},
		true,
	},
	"both = same list": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: toStringList("a", "b")},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: toStringList("a", "b")},
		true,
	},
	"both = subset list": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: toStringList("a")},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: toStringList("a", "b")},
		true,
	},
	"both != same string": {
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}},
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}},
		true,
	},
	"both != same int64": {
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_Int64Value{Int64Value: 100}}},
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_Int64Value{Int64Value: 100}}},
		true,
	},
	"both != same double": {
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_DoubleValue{DoubleValue: 100}}},
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_DoubleValue{DoubleValue: 100}}},
		true,
	},
	"both != same inet": {
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: &QualValue{Value: ToInet("192.168.0.1")}},
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: &QualValue{Value: ToInet("192.168.0.1")}},
		true,
	},
	"both != same bool": {
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_BoolValue{BoolValue: true}}},
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_BoolValue{BoolValue: true}}},
		true,
	},
	"both != same jsonb": {
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_JsonbValue{JsonbValue: "10"}}},
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_JsonbValue{JsonbValue: "10"}}},
		true,
	},
	"both != same timestamp": {
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: getTimestampValue(now)},
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: getTimestampValue(now)},
		true,
	},
	"both != same list": {&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: toStringList("a", "b")},
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f1", Value: toStringList("a", "b")},
		true,
	},
	"both int64 < smaller number": {
		&Qual{Operator: &Qual_StringValue{"<"}, FieldName: "f1", Value: &QualValue{Value: &QualValue_Int64Value{Int64Value: 10}}},
		&Qual{Operator: &Qual_StringValue{"<"}, FieldName: "f1", Value: &QualValue{Value: &QualValue_Int64Value{Int64Value: 100}}},
		true,
	},
	"both int64 <= smaller number": {
		&Qual{Operator: &Qual_StringValue{"<="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_Int64Value{Int64Value: 10}}},
		&Qual{Operator: &Qual_StringValue{"<="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_Int64Value{Int64Value: 100}}},
		true,
	},
	"both int64 > bigger number": {
		&Qual{Operator: &Qual_StringValue{">"}, FieldName: "f1", Value: &QualValue{Value: &QualValue_Int64Value{Int64Value: 100}}},
		&Qual{Operator: &Qual_StringValue{">"}, FieldName: "f1", Value: &QualValue{Value: &QualValue_Int64Value{Int64Value: 10}}},
		true,
	},
	"both int64 >= bigger number": {
		&Qual{Operator: &Qual_StringValue{">="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_Int64Value{Int64Value: 100}}},
		&Qual{Operator: &Qual_StringValue{">="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_Int64Value{Int64Value: 10}}},
		true,
	},
	"both double < smaller number": {
		&Qual{Operator: &Qual_StringValue{"<"}, FieldName: "f1", Value: &QualValue{Value: &QualValue_DoubleValue{DoubleValue: 10}}},
		&Qual{Operator: &Qual_StringValue{"<"}, FieldName: "f1", Value: &QualValue{Value: &QualValue_DoubleValue{DoubleValue: 100}}},
		true,
	},
	"both double <= smaller number": {
		&Qual{Operator: &Qual_StringValue{"<="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_DoubleValue{DoubleValue: 10}}},
		&Qual{Operator: &Qual_StringValue{"<="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_DoubleValue{DoubleValue: 100}}},
		true,
	},
	"both double > bigger number": {
		&Qual{Operator: &Qual_StringValue{">"}, FieldName: "f1", Value: &QualValue{Value: &QualValue_DoubleValue{DoubleValue: 100}}},
		&Qual{Operator: &Qual_StringValue{">"}, FieldName: "f1", Value: &QualValue{Value: &QualValue_DoubleValue{DoubleValue: 10}}},
		true,
	},
	"both double >= bigger number": {
		&Qual{Operator: &Qual_StringValue{">="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_DoubleValue{DoubleValue: 100}}},
		&Qual{Operator: &Qual_StringValue{">="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_DoubleValue{DoubleValue: 10}}},
		true,
	}, "timestamp < earlier": {
		&Qual{Operator: &Qual_StringValue{"<"}, FieldName: "f1", Value: getTimestampValue(now)},
		&Qual{Operator: &Qual_StringValue{"<"}, FieldName: "f1", Value: getTimestampValue(now.Add(1 * time.Second))},
		true,
	},
	"both timestamp <= earlier": {
		&Qual{Operator: &Qual_StringValue{"<="}, FieldName: "f1", Value: getTimestampValue(now)},
		&Qual{Operator: &Qual_StringValue{"<="}, FieldName: "f1", Value: getTimestampValue(now.Add(1 * time.Second))},
		true,
	},
	"both timestamp > later": {
		&Qual{Operator: &Qual_StringValue{">"}, FieldName: "f1", Value: getTimestampValue(now.Add(1 * time.Second))},
		&Qual{Operator: &Qual_StringValue{">"}, FieldName: "f1", Value: getTimestampValue(now)},
		true,
	},
	"both timestamp >= later": {
		&Qual{Operator: &Qual_StringValue{">="}, FieldName: "f1", Value: getTimestampValue(now.Add(1 * time.Second))},
		&Qual{Operator: &Qual_StringValue{">="}, FieldName: "f1", Value: getTimestampValue(now)},
		true,
	},
	"both = same string different field": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f2", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}},
		false,
	},
	"both = same string different operator": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}},
		&Qual{Operator: &Qual_StringValue{"!="}, FieldName: "f2", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}},
		false,
	},
	"different string": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "b"}}},
		false,
	},
	"different int64": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_Int64Value{Int64Value: 100}}},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_Int64Value{Int64Value: 101}}},
		false,
	},
	"different double": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_DoubleValue{DoubleValue: 100}}},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_DoubleValue{DoubleValue: 101}}},
		false,
	},
	"different inet": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: ToInet("192.168.0.1")}},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: ToInet("192.168.0.2")}},
		false,
	},
	"different bool": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_BoolValue{BoolValue: false}}},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_BoolValue{BoolValue: true}}},
		false,
	},
	"different jsonb": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_JsonbValue{JsonbValue: "10"}}},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_JsonbValue{JsonbValue: "101"}}},
		false,
	},
	"different timestamp": {
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: getTimestampValue(now)},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: getTimestampValue(now.Add(1 * time.Second))},
		false,
	},
	"different list": {&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: toStringList("a", "b")},
		&Qual{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: toStringList("a", "b", "c")},
		false,
	},
}

func getTimestampValue(t time.Time) *QualValue {
	return &QualValue{Value: &QualValue_TimestampValue{TimestampValue: &timestamppb.Timestamp{Seconds: t.Unix()}}}

}

func TestIsSubset(t *testing.T) {
	for name, test := range testCasesIsSubset {
		isSubset := test.q1.IsASubsetOf(test.q2)
		if isSubset != test.expected {
			t.Errorf("Test: '%s' FAILED : \nexpected:\n %v \ngot:\n %v\n", name, test.expected, isSubset)
		}
	}
}

func ToInet(ipString string) *QualValue_InetValue {
	var netmaskBits int32 = 0xffff
	protocolVersion := "IPv4"
	return &QualValue_InetValue{
		InetValue: &Inet{
			Mask:            netmaskBits,
			Addr:            ipString,
			Cidr:            fmt.Sprintf("%s/%d", ipString, netmaskBits),
			ProtocolVersion: protocolVersion,
		},
	}
}

func toStringList(items ...string) *QualValue {
	list := make([]*QualValue, len(items))
	for i, item := range items {
		list[i] = &QualValue{Value: &QualValue_StringValue{StringValue: item}}

	}

	return &QualValue{Value: &QualValue_ListValue{ListValue: &QualValueList{Values: list}}}
}

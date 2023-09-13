package proto

import (
	"testing"
	"time"
)

type qualValuesEqualTest struct {
	q1       *QualValue
	q2       *QualValue
	expected bool
}

var testCasesQualValuesEquals = map[string]qualValuesEqualTest{
	"same string": {
		&QualValue{Value: &QualValue_StringValue{StringValue: "a"}},
		&QualValue{Value: &QualValue_StringValue{StringValue: "a"}},
		true,
	},
	"diff string": {
		&QualValue{Value: &QualValue_StringValue{StringValue: "a"}},
		&QualValue{Value: &QualValue_StringValue{StringValue: "b"}},
		false,
	},
	"same int64": {
		&QualValue{Value: &QualValue_Int64Value{Int64Value: 100}},
		&QualValue{Value: &QualValue_Int64Value{Int64Value: 100}},
		true,
	},
	"diff int64": {
		&QualValue{Value: &QualValue_Int64Value{Int64Value: 100}},
		&QualValue{Value: &QualValue_Int64Value{Int64Value: 1000}},
		false,
	},
	"same double": {
		&QualValue{Value: &QualValue_DoubleValue{DoubleValue: 100}},
		&QualValue{Value: &QualValue_DoubleValue{DoubleValue: 100}},
		true,
	},
	"diff double": {
		&QualValue{Value: &QualValue_DoubleValue{DoubleValue: 100.5}},
		&QualValue{Value: &QualValue_DoubleValue{DoubleValue: 100.6}},
		false,
	},
	"same inet": {
		&QualValue{Value: toInet("192.168.0.1")},
		&QualValue{Value: toInet("192.168.0.1")},
		true,
	},
	"diff inet": {
		&QualValue{Value: toInet("192.168.0.1")},
		&QualValue{Value: toInet("192.168.10.1")},
		false,
	},
	"same bool": {
		&QualValue{Value: &QualValue_BoolValue{BoolValue: true}},
		&QualValue{Value: &QualValue_BoolValue{BoolValue: true}},
		true,
	},
	"diff bool": {
		&QualValue{Value: &QualValue_BoolValue{BoolValue: true}},
		&QualValue{Value: &QualValue_BoolValue{BoolValue: false}},
		false,
	},
	"same timestamp": {
		getTimestampValue(now),
		getTimestampValue(now),
		true,
	},
	"diff timestamp": {
		getTimestampValue(now),
		getTimestampValue(now.Add(1 * time.Minute)),
		false,
	},
	"same list": {
		toStringList("a", "b"),
		toStringList("a", "b"),
		true,
	},
	"diff list": {
		toStringList("a", "b"),
		toStringList("a", "c"),
		false,
	},
	"diff types": {
		&QualValue{Value: &QualValue_StringValue{StringValue: "a"}},
		&QualValue{Value: &QualValue_Int64Value{Int64Value: 100}},
		false,
	},
}

func TestQualValuesEquals(t *testing.T) {
	for name, test := range testCasesQualValuesEquals {
		result := test.q1.Equals(test.q2)
		if result != test.expected {
			t.Errorf("Test: '%s' FAILED : \nexpected:\n %v \ngot:\n %v\n", name, test.expected, result)
		}
	}
}

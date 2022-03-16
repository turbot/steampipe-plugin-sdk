package proto

import "testing"

// TODO add equals
// matching same order
// matching diff order
// not matching same length
// not matching 'other' shorter
// not matching 'other' longer

type isEqualTest struct {
	q1       *Quals
	q2       *Quals
	expected bool
}

var testCasesQuals = map[string]isEqualTest{
	"diff length(q2 longer)": {
		q1: &Quals{
			Quals: []*Qual{{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}}, {Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}}},
		},
		q2: &Quals{
			Quals: []*Qual{{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}}, {Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "b"}}}, {Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "b"}}}},
		},
		expected: false,
	},
	"diff length(q2 shorter)": {
		q1: &Quals{
			Quals: []*Qual{{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}}, {Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}}},
		},
		q2: &Quals{
			Quals: []*Qual{{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}}},
		},
		expected: false,
	},
	"same length, same order": {
		q1: &Quals{
			Quals: []*Qual{{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}}, {Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "b"}}}},
		},
		q2: &Quals{
			Quals: []*Qual{{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}}, {Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "b"}}}},
		},
		expected: true,
	},
	"same length, diff order": {
		q1: &Quals{
			Quals: []*Qual{{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}}, {Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "b"}}}},
		},
		q2: &Quals{
			Quals: []*Qual{{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "b"}}}, {Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}}},
		},
		expected: true,
	},
	"same length, diff quals": {
		q1: &Quals{
			Quals: []*Qual{{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}}, {Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "b"}}}},
		},
		q2: &Quals{
			Quals: []*Qual{{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "c"}}}, {Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "d"}}}},
		},
		expected: false,
	},
	"same length, diff quals 2": {
		q1: &Quals{
			Quals: []*Qual{{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}}, {Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}}},
		},
		q2: &Quals{
			Quals: []*Qual{{Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "a"}}}, {Operator: &Qual_StringValue{"="}, FieldName: "f1", Value: &QualValue{Value: &QualValue_StringValue{StringValue: "d"}}}},
		},
		expected: false,
	},
}

func TestQuals(t *testing.T) {
	for name, test := range testCasesQuals {
		result := test.q1.Equals(test.q2)
		if result != test.expected {
			t.Errorf("Test: '%s' FAILED : \nexpected:\n %v \ngot:\n %v\n", name, test.expected, result)
		}
	}
}

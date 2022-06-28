package transform

import (
	"context"
	"reflect"
	"testing"
)

type ColumnDataTransformTest struct {
	transformData *TransformData
	expected      interface{}
	transforms    *ColumnTransforms
}

var textCtx = context.Background()

// struct mimicking api return data
type apiStruct struct {
	S string
	I int
}

var testCasesColumnDataTransform = map[string]ColumnDataTransformTest{
	"FromField (hydrate item, map source)": {
		transformData: &TransformData{
			HydrateItem: map[string]string{"S": "stringval"},

			ColumnName: "c1",
		},
		transforms: FromField("S"),
		expected:   "stringval",
	},
	"FromField (hydrate item, struct source)": {
		transformData: &TransformData{
			HydrateItem: &apiStruct{S: "stringval"},
			ColumnName:  "c1",
		},
		transforms: FromField("S"),
		expected:   "stringval",
	},
}

func TestColumnDataTransform(t *testing.T) {
	for name, test := range testCasesColumnDataTransform {
		result, err := test.transforms.Execute(textCtx, test.transformData)
		if err != nil {
			if test.expected != "ERROR" {
				t.Errorf("Test: '%s'' FAILED : \nunextected error %v", name, err)
			}
			continue
		}

		if !reflect.DeepEqual(test.expected, result) {
			t.Errorf("Test: '%s'' FAILED : \nexpected:\n %v, \n\ngot:\n %v", name, test.expected, result)
		}
	}
}

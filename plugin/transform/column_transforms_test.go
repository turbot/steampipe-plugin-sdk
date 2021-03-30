package transform

import (
	"context"
	"reflect"
	"testing"
)

type ColumnDataTransformTest struct {
	hydrateItem interface{}
	transforms  *ColumnTransforms
	column      string
	expected    interface{}
}

var textCtx = context.Background()

// struct mimicking api return data
type apiStruct struct {
	S string
	I int
}

var testCasesColumnDataTransform = map[string]ColumnDataTransformTest{

	"FromField (hydrate item, map source)": {
		hydrateItem: map[string]string{"S": "stringval"},
		transforms:  FromField("S"),
		column:      "c1",
		expected:    "stringval",
	},
	"FromField (hydrate item, struct source)": {
		hydrateItem: &apiStruct{S: "stringval"},
		transforms:  FromField("S"),
		column:      "c1",
		expected:    "stringval",
	},
}

func TestColumnDataTransform(t *testing.T) {
	for name, test := range testCasesColumnDataTransform {
		result, err := test.transforms.Execute(textCtx, test.hydrateItem, nil, nil, test.column)
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

package plugin

import (
	"context"
	"fmt"
	"github.com/turbotio/go-kit/helpers"
	"github.com/turbotio/steampipe-plugin-sdk/grpc/proto"
	"strings"
	"testing"
)

//// isGet ////

type isGetTest struct {
	table    Table
	quals    map[string]*proto.Quals
	expected interface{}
}

type isGetTestResult struct {
	keyValues map[string]*proto.QualValue
	isGet     bool
}

func testGet(context.Context, *QueryData, *HydrateData) (interface{}, error) {
	return nil, nil
}
func testList(context.Context, *QueryData, *HydrateData) (interface{}, error) {
	return nil, nil

}

var testCasesIsGet = map[string]isGetTest{
	"no keyColumn": {
		table: Table{
			Name:    "aws_s3_bucket",
			Columns: []*Column{},
		},
		quals:    map[string]*proto.Quals{"name": {Quals: []*proto.Qual{{Operator: &proto.Qual_StringValue{StringValue: "="}, FieldName: "name", Value: &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: "dave"}}}}}},
		expected: isGetTestResult{nil, false},
	},
	"single keyColumn, single equals constraint": {
		table: Table{
			Name: "aws_s3_bucket",
			Get: &GetConfig{
				KeyColumns: SingleColumn("name"),
				Hydrate:    testGet,
			},
			Columns: []*Column{},
			List: &ListConfig{
				Hydrate: testList,
			},
		},
		quals:    map[string]*proto.Quals{"name": {Quals: []*proto.Qual{{Operator: &proto.Qual_StringValue{StringValue: "="}, FieldName: "name", Value: &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: "dave"}}}}}},
		expected: isGetTestResult{map[string]*proto.QualValue{"name": {Value: &proto.QualValue_StringValue{StringValue: "dave"}}}, true},
	},

	"single keyColumn, no constraint": {
		table: Table{
			Name: "aws_s3_bucket",
			Get: &GetConfig{
				KeyColumns: SingleColumn("name"),
				Hydrate:    testGet,
			},
			Columns: []*Column{},
		},
		quals:    map[string]*proto.Quals{},
		expected: isGetTestResult{nil, false},
	},
	"single keyColumn, single equals constraint and other unrelated constraint": {
		table: Table{
			Name: "aws_s3_bucket",
			Get: &GetConfig{
				KeyColumns: SingleColumn("name"),
				Hydrate:    testGet,
			},
			Columns: []*Column{},
		},
		quals: map[string]*proto.Quals{
			"name": {Quals: []*proto.Qual{{Operator: &proto.Qual_StringValue{StringValue: "="}, FieldName: "name", Value: &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: "dave"}}}}},
			"bar":  {Quals: []*proto.Qual{{Operator: &proto.Qual_StringValue{StringValue: "="}, FieldName: "bar", Value: &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: "foo"}}}}}},
		expected: isGetTestResult{map[string]*proto.QualValue{"name": {Value: &proto.QualValue_StringValue{StringValue: "dave"}}}, true},
	},
	"all keyColumns, single equals constraint for each": {
		table: Table{
			Name: "aws_s3_bucket",
			Get: &GetConfig{
				KeyColumns: AllColumns([]string{"name", "age"}),
				Hydrate:    testGet,
			},

			Columns: []*Column{},
		},
		quals: map[string]*proto.Quals{
			"name": {Quals: []*proto.Qual{{Operator: &proto.Qual_StringValue{StringValue: "="}, FieldName: "name", Value: &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: "dave"}}}}},
			"age":  {Quals: []*proto.Qual{{Operator: &proto.Qual_StringValue{StringValue: "="}, FieldName: "age", Value: &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: "100"}}}}}},
		expected: isGetTestResult{map[string]*proto.QualValue{
			"name": {Value: &proto.QualValue_StringValue{StringValue: "dave"}},
			"age":  {Value: &proto.QualValue_StringValue{StringValue: "100"}}}, true},
	},
	"all keyColumns, single equals constraint for one": {
		table: Table{
			Name: "aws_s3_bucket",
			Get: &GetConfig{
				KeyColumns: AllColumns([]string{"name", "age"}),
				Hydrate:    testGet,
			},
			Columns: []*Column{},
		},
		quals: map[string]*proto.Quals{
			"name": {Quals: []*proto.Qual{{Operator: &proto.Qual_StringValue{StringValue: "="}, FieldName: "name", Value: &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: "dave"}}}}}},
		expected: isGetTestResult{nil, false},
	},
	"any keyColumns, single equals constraint for one": {
		table: Table{
			Name: "aws_s3_bucket",
			Get: &GetConfig{
				KeyColumns: AnyColumn([]string{"name", "age"}),
				Hydrate:    testGet,
			},
			Columns: []*Column{},
		},
		quals: map[string]*proto.Quals{
			"name": {Quals: []*proto.Qual{{Operator: &proto.Qual_StringValue{StringValue: "="}, FieldName: "name", Value: &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: "dave"}}}}}},
		expected: isGetTestResult{map[string]*proto.QualValue{"name": {Value: &proto.QualValue_StringValue{StringValue: "dave"}}}, true},
	},
	"any keyColumns, single equals constraint for both": {
		table: Table{
			Name: "aws_s3_bucket",
			Get: &GetConfig{
				KeyColumns: AnyColumn([]string{"name", "age"}),
				Hydrate:    testGet,
			},
			Columns: []*Column{},
		},
		quals: map[string]*proto.Quals{
			"name": {Quals: []*proto.Qual{{Operator: &proto.Qual_StringValue{StringValue: "="}, FieldName: "name", Value: &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: "dave"}}}}},
			"age":  {Quals: []*proto.Qual{{Operator: &proto.Qual_StringValue{StringValue: "="}, FieldName: "age", Value: &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: "100"}}}}}},
		expected: isGetTestResult{map[string]*proto.QualValue{"name": {Value: &proto.QualValue_StringValue{StringValue: "dave"}}}, true},
	},
	"any keyColumns, no quals": {
		table: Table{
			Name: "aws_s3_bucket",
			Get: &GetConfig{
				KeyColumns: AnyColumn([]string{"name", "age"}),
				Hydrate:    testGet,
			},
			Columns: []*Column{},
		},
		quals:    map[string]*proto.Quals{},
		expected: isGetTestResult{nil, false},
	},
}

func TestIsGet(t *testing.T) {
	//for name, test := range testCasesIsGet {
	//	QueryContext := &proto.QueryContext{
	//		Quals: test.quals,
	//	}
	//	//dataModel := CreateDataModel(&test.table, QueryContext, nil, hclog.New(&hclog.LoggerOptions{}))
	//
	//	keyValues, getFetchType := dataModel.getFetchType()
	//	result := isGetTestResult{keyValues, getFetchType}
	//
	//	expected := test.expected.(isGetTestResult)
	//	if !reflect.DeepEqual(expected, result) {
	//		t.Errorf(`Test: '%s'' FAILED : expected %v, got %v`, name, test.expected, keyValues)
	//	}
	//}
}

//// requiredHydrateCallBuilder ////

// declared in plugin_test
// listHydrate, getHydrate, hydrate1, hydrate2, hydrate3, hydrate4

type requiredHydrateCallsTest struct {
	table     Table
	columns   []string
	fetchType fetchType
	expected  []*HydrateCall
}

var testCasesRequiredHydrateCalls = map[string]requiredHydrateCallsTest{
	"list no hydrate": {
		table: Table{
			Name: "table",
			Columns: []*Column{
				{Name: "c1"},
				{Name: "c2"},
			},
			List: &ListConfig{
				Hydrate: listHydrate,
			},
			Get: &GetConfig{
				Hydrate: getHydrate,
			},
			HydrateDependencies: nil,
		},
		columns:   []string{"c1"},
		fetchType: fetchTypeList,
		expected:  []*HydrateCall{},
	},
	"list - 1 hydrate": {
		table: Table{
			Name: "table",
			Columns: []*Column{
				{Name: "c1", Hydrate: hydrate1},
				{Name: "c2"},
			},
			List:                &ListConfig{Hydrate: listHydrate},
			Get:                 &GetConfig{Hydrate: getHydrate},
			HydrateDependencies: nil,
		},
		columns:   []string{"c1"},
		fetchType: fetchTypeList,
		expected:  []*HydrateCall{{Func: hydrate1}},
	},
	"list - 1 hydrate, depends": {
		table: Table{
			Name: "table",
			Columns: []*Column{
				{Name: "c1", Hydrate: hydrate1},
				{Name: "c2"},
			},
			List:                &ListConfig{Hydrate: listHydrate},
			Get:                 &GetConfig{Hydrate: getHydrate},
			HydrateDependencies: []HydrateDependencies{{hydrate1, []HydrateFunc{hydrate2}}},
		},
		columns:   []string{"c1"},
		fetchType: fetchTypeList,
		expected:  []*HydrateCall{{Func: hydrate1, Depends: []string{"hydrate2"}}, {Func: hydrate2}},
	},
	"get - 2 hydrate, depends": {
		table: Table{
			Name: "table",
			Columns: []*Column{
				{Name: "c1", Hydrate: hydrate1},
				{Name: "c2", Hydrate: hydrate2},
				{Name: "c3", Hydrate: hydrate3},
			},
			List:                &ListConfig{Hydrate: listHydrate},
			Get:                 &GetConfig{Hydrate: getHydrate},
			HydrateDependencies: []HydrateDependencies{{hydrate1, []HydrateFunc{hydrate3}}},
		},
		columns:   []string{"c1", "c2"},
		fetchType: fetchTypeGet,
		expected:  []*HydrateCall{{Func: hydrate1, Depends: []string{"hydrate3"}}, {Func: hydrate3}, {Func: hydrate2}},
	},
	"get - 2 depends": {
		table: Table{
			Name: "table",
			Columns: []*Column{
				{Name: "c1", Hydrate: hydrate1},
				{Name: "c2", Hydrate: hydrate2},
				{Name: "c3", Hydrate: hydrate3},
			},
			List: &ListConfig{Hydrate: listHydrate},
			Get:  &GetConfig{Hydrate: getHydrate},
			HydrateDependencies: []HydrateDependencies{
				{hydrate1, []HydrateFunc{hydrate2}},
				{hydrate2, []HydrateFunc{hydrate3}},
			},
		},
		columns:   []string{"c1"},
		fetchType: fetchTypeGet,
		expected:  []*HydrateCall{{Func: hydrate1, Depends: []string{"hydrate2"}}, {Func: hydrate2, Depends: []string{"hydrate3"}}, {Func: hydrate3}},
	},
	"get - unreferenced depends": {
		table: Table{
			Name: "table",
			Columns: []*Column{
				{Name: "c1", Hydrate: hydrate1},
				{Name: "c2", Hydrate: hydrate2},
				{Name: "c3", Hydrate: hydrate3},
			},
			List: &ListConfig{Hydrate: listHydrate},
			Get:  &GetConfig{Hydrate: getHydrate},
			HydrateDependencies: []HydrateDependencies{
				{hydrate1, []HydrateFunc{hydrate2}},
				{hydrate2, []HydrateFunc{hydrate3}},
			},
		},
		columns:   []string{"c3"},
		fetchType: fetchTypeGet,
		expected:  []*HydrateCall{{Func: hydrate3}},
	},
}

func TestRequiredHydrateCalls(t *testing.T) {
	for name, test := range testCasesRequiredHydrateCalls {
		result := test.table.requiredHydrateCalls(test.columns, test.fetchType)

		if len(test.expected) == 0 && len(result) == 0 {
			continue
		}
		expectedString := hydrateArrayToString(test.expected)
		actualString := hydrateArrayToString(result)
		if expectedString != actualString {
			t.Errorf("Test: '%s'' FAILED : expected \n%v\ngot \n%v", name, expectedString, actualString)
		}
	}
}

func hydrateArrayToString(calls []*HydrateCall) string {
	var strs []string
	for _, c := range calls {
		strs = append(strs, hydrateCallToString(c))
	}
	return strings.Join(strs, "\n")
}

func hydrateCallToString(call *HydrateCall) string {
	str := fmt.Sprintf("Func: %s", helpers.GetFunctionName(call.Func))
	if len(call.Depends) > 0 {
		str += "\n  Depends:"
	}

	for _, c := range call.Depends {
		str += fmt.Sprintf("%s\n          ", c)
	}
	return str
}

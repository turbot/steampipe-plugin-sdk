package plugin

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
)

// isGet

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

// requiredHydrateCallBuilder

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
			Plugin:              &Plugin{},
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
			Plugin:              &Plugin{},
		},
		columns:   []string{"c1"},
		fetchType: fetchTypeList,
		expected:  []*HydrateCall{{Func: hydrate1, Name: "hydrate1"}},
	},
	"list - 1 hydrate, depends [HydrateDependencies]": {
		table: Table{
			Name: "table",
			Columns: []*Column{
				{Name: "c1", Hydrate: hydrate1},
				{Name: "c2"},
			},
			List:                &ListConfig{Hydrate: listHydrate},
			Get:                 &GetConfig{Hydrate: getHydrate},
			HydrateDependencies: []HydrateDependencies{{Func: hydrate1, Depends: []HydrateFunc{hydrate2}}},
			Plugin:              &Plugin{},
		},
		columns:   []string{"c1"},
		fetchType: fetchTypeList,
		expected:  []*HydrateCall{{Func: hydrate1, Name: "hydrate1", Depends: []string{"hydrate2"}}, {Func: hydrate2, Name: "hydrate2"}},
	},
	"get - 2 hydrate, depends [HydrateDependencies]": {
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
			Plugin:              &Plugin{},
		},
		columns:   []string{"c1", "c2"},
		fetchType: fetchTypeGet,
		expected:  []*HydrateCall{{Func: hydrate1, Name: "hydrate1", Depends: []string{"hydrate3"}}, {Func: hydrate3, Name: "hydrate3"}, {Func: hydrate2, Name: "hydrate2"}},
	},
	"get - 2 depends [HydrateDependencies]": {
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
			Plugin: &Plugin{},
		},
		columns:   []string{"c1"},
		fetchType: fetchTypeGet,
		expected:  []*HydrateCall{{Func: hydrate1, Name: "hydrate1", Depends: []string{"hydrate2"}}, {Func: hydrate2, Name: "hydrate2", Depends: []string{"hydrate3"}}, {Func: hydrate3, Name: "hydrate3"}},
	},
	"get - unreferenced depends [HydrateDependencies]": {
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
			Plugin: &Plugin{},
		},
		columns:   []string{"c3"},
		fetchType: fetchTypeGet,
		expected:  []*HydrateCall{{Func: hydrate3, Name: "hydrate3"}},
	},

	"list - 1 hydrate, depends": {
		table: Table{
			Name: "table",
			Columns: []*Column{
				{Name: "c1", Hydrate: hydrate1},
				{Name: "c2"},
			},
			List:          &ListConfig{Hydrate: listHydrate},
			Get:           &GetConfig{Hydrate: getHydrate},
			HydrateConfig: []HydrateConfig{{Func: hydrate1, Depends: []HydrateFunc{hydrate2}}},
			Plugin:        &Plugin{},
		},
		columns:   []string{"c1"},
		fetchType: fetchTypeList,
		expected:  []*HydrateCall{{Func: hydrate1, Name: "hydrate1", Depends: []string{"hydrate2"}}, {Func: hydrate2, Name: "hydrate2"}},
	},
	"get - 2 hydrate, depends": {
		table: Table{
			Name: "table",
			Columns: []*Column{
				{Name: "c1", Hydrate: hydrate1},
				{Name: "c2", Hydrate: hydrate2},
				{Name: "c3", Hydrate: hydrate3},
			},
			List:          &ListConfig{Hydrate: listHydrate},
			Get:           &GetConfig{Hydrate: getHydrate},
			HydrateConfig: []HydrateConfig{{Func: hydrate1, Depends: []HydrateFunc{hydrate3}}},
			Plugin:        &Plugin{},
		},
		columns:   []string{"c1", "c2"},
		fetchType: fetchTypeGet,
		expected:  []*HydrateCall{{Func: hydrate1, Name: "hydrate1", Depends: []string{"hydrate3"}}, {Func: hydrate3, Name: "hydrate3"}, {Func: hydrate2, Name: "hydrate2"}},
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
			HydrateConfig: []HydrateConfig{
				{Func: hydrate1, Depends: []HydrateFunc{hydrate2}},
				{Func: hydrate2, Depends: []HydrateFunc{hydrate3}},
			},
			Plugin: &Plugin{},
		},
		columns:   []string{"c1"},
		fetchType: fetchTypeGet,
		expected:  []*HydrateCall{{Func: hydrate1, Name: "hydrate1", Depends: []string{"hydrate2"}}, {Func: hydrate2, Name: "hydrate2", Depends: []string{"hydrate3"}}, {Func: hydrate3, Name: "hydrate3"}},
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
			HydrateConfig: []HydrateConfig{
				{Func: hydrate1, Depends: []HydrateFunc{hydrate2}},
				{Func: hydrate2, Depends: []HydrateFunc{hydrate3}},
			},
			Plugin: &Plugin{},
		},
		columns:   []string{"c3"},
		fetchType: fetchTypeGet,
		expected:  []*HydrateCall{{Func: hydrate3, Name: "hydrate3"}},
	},
}

func TestRequiredHydrateCalls(t *testing.T) {
	plugin := &Plugin{}
	plugin.Initialise()
	for name, test := range testCasesRequiredHydrateCalls {
		test.table.initialise(plugin)
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
	sort.Strings(strs)
	return strings.Join(strs, "\n")
}

func hydrateCallToString(call *HydrateCall) string {
	str := fmt.Sprintf("Func: %s", call.Name)
	if len(call.Depends) > 0 {
		str += "\n  Depends:"
	}

	for _, c := range call.Depends {
		str += fmt.Sprintf("%s\n          ", c)
	}
	return str
}

// getHydrateConfig

type getHydrateConfigTest struct {
	table    *Table
	funcName string
	expected *HydrateConfig
}

var getHydrateConfigTestTableWithDefaults = &Table{
	Name:                     "test",
	Plugin:                   getHydrateConfigTestPlugin,
	DefaultShouldIgnoreError: shouldIgnoreErrorTableDefault,
	DefaultRetryConfig: &RetryConfig{
		ShouldRetryError: shouldRetryErrorTableDefault,
	},
	HydrateConfig: []HydrateConfig{
		{
			Func:           hydrate1,
			MaxConcurrency: 1,
			Depends:        []HydrateFunc{hydrate2, hydrate3},
		},
		{
			Func:              hydrate2,
			MaxConcurrency:    2,
			ShouldIgnoreError: shouldIgnoreError1,
		},
		{
			Func:           hydrate3,
			MaxConcurrency: 3,
			RetryConfig:    &RetryConfig{ShouldRetryError: shouldRetryError1},
		},
		{
			Func:              hydrate4,
			MaxConcurrency:    4,
			ShouldIgnoreError: shouldIgnoreError2,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryError2},
		},
	},
}

var getHydrateConfigTestTableNoDefaults = &Table{
	Name:   "test",
	Plugin: getHydrateConfigTestPlugin,
	HydrateConfig: []HydrateConfig{
		{
			Func:           hydrate1,
			MaxConcurrency: 1,
			Depends:        []HydrateFunc{hydrate2, hydrate3},
		},
		{
			Func:              hydrate2,
			MaxConcurrency:    2,
			ShouldIgnoreError: shouldIgnoreError1,
		},
		{
			Func:           hydrate3,
			MaxConcurrency: 3,
			RetryConfig:    &RetryConfig{ShouldRetryError: shouldRetryError1},
		},
	},
}

var getHydrateConfigTestPlugin = &Plugin{
	Name:                     "test",
	DefaultShouldIgnoreError: shouldIgnoreErrorPluginDefault,
	DefaultRetryConfig: &RetryConfig{
		ShouldRetryError: shouldRetryErrorPluginDefault,
	},
}

var testCasesGetHydrateConfig = map[string]getHydrateConfigTest{
	"tables default retry and should ignore": {
		table:    getHydrateConfigTestTableWithDefaults,
		funcName: "hydrate1",

		expected: &HydrateConfig{
			Func:              hydrate1,
			MaxConcurrency:    1,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryErrorTableDefault},
			ShouldIgnoreError: shouldIgnoreErrorTableDefault,
			IgnoreConfig:      &IgnoreConfig{ShouldIgnoreError: shouldIgnoreErrorTableDefault},
			Depends:           []HydrateFunc{hydrate2, hydrate3},
		},
	},
	"table default retry": {
		table:    getHydrateConfigTestTableWithDefaults,
		funcName: "hydrate2",

		expected: &HydrateConfig{
			Func:              hydrate2,
			MaxConcurrency:    2,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryErrorTableDefault},
			ShouldIgnoreError: shouldIgnoreError1,
			IgnoreConfig:      &IgnoreConfig{ShouldIgnoreError: shouldIgnoreError1},
		},
	},
	"tables default should ignore": {
		table:    getHydrateConfigTestTableWithDefaults,
		funcName: "hydrate3",

		expected: &HydrateConfig{
			Func:              hydrate3,
			MaxConcurrency:    3,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryError1},
			ShouldIgnoreError: shouldIgnoreErrorTableDefault,
			IgnoreConfig:      &IgnoreConfig{ShouldIgnoreError: shouldIgnoreErrorTableDefault},
		},
	},
	"plugin default retry and should ignore": {
		table:    getHydrateConfigTestTableNoDefaults,
		funcName: "hydrate1",

		expected: &HydrateConfig{
			Func:              hydrate1,
			MaxConcurrency:    1,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryErrorPluginDefault},
			ShouldIgnoreError: shouldIgnoreErrorPluginDefault,
			IgnoreConfig:      &IgnoreConfig{ShouldIgnoreError: shouldIgnoreErrorPluginDefault},
			Depends:           []HydrateFunc{hydrate2, hydrate3},
		},
	},
	"plugin default retry": {
		table:    getHydrateConfigTestTableNoDefaults,
		funcName: "hydrate2",

		expected: &HydrateConfig{
			Func:              hydrate2,
			MaxConcurrency:    2,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryErrorPluginDefault},
			ShouldIgnoreError: shouldIgnoreError1,
			IgnoreConfig:      &IgnoreConfig{ShouldIgnoreError: shouldIgnoreError1},
		},
	},
	"plugin default should ignore": {
		table:    getHydrateConfigTestTableNoDefaults,
		funcName: "hydrate3",

		expected: &HydrateConfig{
			Func:              hydrate3,
			MaxConcurrency:    3,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryError1},
			ShouldIgnoreError: shouldIgnoreErrorPluginDefault,
			IgnoreConfig:      &IgnoreConfig{ShouldIgnoreError: shouldIgnoreErrorPluginDefault},
		},
	},
	"no defaults": {
		table:    getHydrateConfigTestTableWithDefaults,
		funcName: "hydrate4",

		expected: &HydrateConfig{
			Func:              hydrate4,
			MaxConcurrency:    4,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryError2},
			ShouldIgnoreError: shouldIgnoreError2,
			IgnoreConfig:      &IgnoreConfig{ShouldIgnoreError: shouldIgnoreError2},
		},
	},
}

func shouldIgnoreErrorTableDefault(err error) bool {
	return true
}
func shouldIgnoreErrorPluginDefault(err error) bool {
	return true
}
func shouldIgnoreError1(error) bool {
	return true
}
func shouldIgnoreError2(error) bool {
	return true
}
func shouldRetryErrorTableDefault(error) bool {
	return true
}
func shouldRetryErrorPluginDefault(error) bool {
	return true
}
func shouldRetryError1(error) bool {
	return true
}
func shouldRetryError2(error) bool {
	return true
}

func TestGetHydrateConfig(t *testing.T) {

	for name, test := range testCasesGetHydrateConfig {
		test.table.Plugin.Initialise()
		test.table.initialise(test.table.Plugin)

		result := test.table.getHydrateConfig(test.funcName)
		actualString := result.String()
		expectedString := test.expected.String()
		if expectedString != actualString {
			t.Errorf("Test: '%s'' FAILED : expected: \n\n%v\n\ngot: \n\n%v", name, expectedString, actualString)
		}
	}
}

// validate table

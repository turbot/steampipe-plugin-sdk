package plugin

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// isGet

type isGetTest struct {
	table    *Table
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
		table: &Table{
			Name:    "aws_s3_bucket",
			Columns: []*Column{},
		},
		quals:    map[string]*proto.Quals{"name": {Quals: []*proto.Qual{{Operator: &proto.Qual_StringValue{StringValue: "="}, FieldName: "name", Value: &proto.QualValue{Value: &proto.QualValue_StringValue{StringValue: "dave"}}}}}},
		expected: isGetTestResult{nil, false},
	},
	"single keyColumn, single equals constraint": {
		table: &Table{
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
		table: &Table{
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
		table: &Table{
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
		table: &Table{
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
		table: &Table{
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
		table: &Table{
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
		table: &Table{
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
		table: &Table{
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
	table     *Table
	columns   []string
	fetchType fetchType
	expected  []*hydrateCall
}

var testCasesRequiredHydrateCalls = map[string]requiredHydrateCallsTest{
	"list no hydrate": {
		table: &Table{
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
		expected:  []*hydrateCall{},
	},
	"list - 1 hydrate": {
		table: &Table{
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
		expected: []*hydrateCall{
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate1, Name: "hydrate1"}},
		},
	},
	"list - 1 hydrate, depends [HydrateDependencies]": {
		table: &Table{
			Name: "table",
			Columns: []*Column{
				{Name: "c1", Hydrate: hydrate1},
				{Name: "c2", Hydrate: hydrate2},
			},
			List:                &ListConfig{Hydrate: listHydrate},
			Get:                 &GetConfig{Hydrate: getHydrate},
			HydrateDependencies: []HydrateDependencies{{Func: hydrate1, Depends: []HydrateFunc{hydrate2}}},
			Plugin:              &Plugin{},
		},
		columns:   []string{"c1"},
		fetchType: fetchTypeList,
		expected: []*hydrateCall{
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate1, Name: "hydrate1"}, Depends: []namedHydrateFunc{{Name: "hydrate2"}}},
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate2, Name: "hydrate2"}},
		},
	},
	"get - 2 hydrate, depends [HydrateDependencies]": {
		table: &Table{
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
		expected: []*hydrateCall{
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate1, Name: "hydrate1"}, Depends: []namedHydrateFunc{{Name: "hydrate3"}}},
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate3, Name: "hydrate3"}},
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate2, Name: "hydrate2"}}},
	},
	"get - 2 depends [HydrateDependencies]": {
		table: &Table{
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
		expected: []*hydrateCall{
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate1, Name: "hydrate1"}, Depends: []namedHydrateFunc{{Name: "hydrate2"}}},
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate2, Name: "hydrate2"}, Depends: []namedHydrateFunc{{Name: "hydrate3"}}},
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate3, Name: "hydrate3"}}},
	},
	"get - unreferenced depends [HydrateDependencies]": {
		table: &Table{
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
		expected: []*hydrateCall{
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate3, Name: "hydrate3"}}},
	},

	"list - 1 hydrate, depends": {
		table: &Table{
			Name: "table",
			Columns: []*Column{
				{Name: "c1", Hydrate: hydrate1},
				{Name: "c2", Hydrate: hydrate2},
			},
			List:          &ListConfig{Hydrate: listHydrate},
			Get:           &GetConfig{Hydrate: getHydrate},
			HydrateConfig: []HydrateConfig{{Func: hydrate1, Depends: []HydrateFunc{hydrate2}}},
			Plugin:        &Plugin{},
		},
		columns:   []string{"c1"},
		fetchType: fetchTypeList,
		expected: []*hydrateCall{
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate1, Name: "hydrate1"}, Depends: []namedHydrateFunc{{Name: "hydrate2"}}},
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate2, Name: "hydrate2"}}},
	},
	"get - 2 hydrate, depends": {
		table: &Table{
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
		expected: []*hydrateCall{
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate1, Name: "hydrate1"}, Depends: []namedHydrateFunc{{Name: "hydrate3"}}},
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate3, Name: "hydrate3"}},
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate2, Name: "hydrate2"}},
		},
	},
	"get - 2 depends": {
		table: &Table{
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
		expected: []*hydrateCall{
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate1, Name: "hydrate1"}, Depends: []namedHydrateFunc{{Name: "hydrate2"}}},
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate2, Name: "hydrate2"}, Depends: []namedHydrateFunc{{Name: "hydrate3"}}},
			{namedHydrateFunc: namedHydrateFunc{Func: hydrate3, Name: "hydrate3"}},
		},
	},
	"get - unreferenced depends": {
		table: &Table{
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
		expected:  []*hydrateCall{{namedHydrateFunc: namedHydrateFunc{Func: hydrate3, Name: "hydrate3"}}},
	},
}

func TestRequiredHydrateCalls(t *testing.T) {
	plugin := &Plugin{}
	logger := hclog.NewNullLogger()
	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))

	plugin.initialise(logger)
	for name, test := range testCasesRequiredHydrateCalls {
		test.table.initialise(plugin)

		d, _ := newTestQueryData(plugin, &QueryContext{Columns: test.columns}, test.table, test.fetchType)
		result := d.hydrateCalls

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
func newTestQueryData(plugin *Plugin, queryContext *QueryContext, table *Table, fetchType fetchType) (*QueryData, error) {
	d := &QueryData{
		Table:        table,
		QueryContext: queryContext,
		//Connection:        connectionData.Connection,
		EqualsQuals: make(map[string]*proto.QualValue),
		Quals:       make(KeyColumnQualMap),
		FetchType:   fetchType,
		plugin:      plugin,
		columns:     make(map[string]*QueryColumn),

		// asyncronously read items using the 'get' or 'list' API
		// items are streamed on rowDataChan, errors returned on errorChan
	}
	d.setFetchType(table)

	// NOTE: for count(*) queries, there will be no columns - add in 1 column so that we have some data to return
	queryContext.ensureColumns(table)

	// build list of required hydrate calls, based on requested columns
	d.populateRequiredHydrateCalls()
	// build list of all columns returned by these hydrate calls (and the fetch call)
	d.populateColumns()

	// populate the query status
	// if a limit is set, use this to set rows required - otherwise just set to MaxInt32
	d.queryStatus = newQueryStatus(d.QueryContext.Limit)

	return d, nil
}

func hydrateArrayToString(calls []*hydrateCall) string {
	var strs []string
	for _, c := range calls {
		strs = append(strs, hydrateCallToString(c))
	}
	sort.Strings(strs)
	return strings.Join(strs, "\n")
}

func hydrateCallToString(call *hydrateCall) string {
	str := fmt.Sprintf("Func: %s", call.Name)
	if len(call.Depends) > 0 {
		str += "\n  Depends:"
	}

	for _, c := range call.Depends {
		str += fmt.Sprintf("%s\n          ", c.Name)
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
	Name:   "test",
	Plugin: getHydrateConfigTestPlugin,
	DefaultIgnoreConfig: &IgnoreConfig{
		ShouldIgnoreErrorFunc: shouldIgnoreErrorTableDefault,
	},
	DefaultRetryConfig: &RetryConfig{
		ShouldRetryErrorFunc: shouldRetryErrorTableDefault,
	},
	HydrateConfig: []HydrateConfig{
		{
			Func:           hydrate1,
			MaxConcurrency: 1,
			Depends:        []HydrateFunc{hydrate2, hydrate3},
		},
		{
			Func:           hydrate2,
			MaxConcurrency: 2,
			IgnoreConfig:   &IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreError1},
		},
		{
			Func:           hydrate3,
			MaxConcurrency: 3,
			RetryConfig:    &RetryConfig{ShouldRetryErrorFunc: shouldRetryError1},
		},
		{
			Func:           hydrate4,
			MaxConcurrency: 4,
			RetryConfig:    &RetryConfig{ShouldRetryErrorFunc: shouldRetryError2},
			IgnoreConfig:   &IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreError2},
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
			Func:           hydrate2,
			MaxConcurrency: 2,
			IgnoreConfig:   &IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreError1},
		},
		{
			Func:           hydrate3,
			MaxConcurrency: 3,
			RetryConfig:    &RetryConfig{ShouldRetryErrorFunc: shouldRetryError1},
		},
	},
}
var getHydrateConfigTestPlugin = &Plugin{
	Name: "test",

	DefaultIgnoreConfig: &IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreErrorPluginDefault},
	DefaultRetryConfig: &RetryConfig{
		ShouldRetryErrorFunc: shouldRetryErrorPluginDefault,
	},
}

// legacy structs
var getHydrateConfigTestPluginLegacy = &Plugin{
	Name:                     "test",
	DefaultShouldIgnoreError: shouldIgnoreErrorPluginDefaultLegacy,
	DefaultRetryConfig: &RetryConfig{
		ShouldRetryError: shouldRetryErrorPluginDefaultLegacy,
	},
}
var getHydrateConfigTestTableWithDefaultsLegacy = &Table{
	Name:                     "test",
	Plugin:                   getHydrateConfigTestPluginLegacy,
	DefaultShouldIgnoreError: shouldIgnoreErrorTableDefaultLegacy,
	DefaultRetryConfig: &RetryConfig{
		ShouldRetryError: shouldRetryErrorTableDefaultLegacy,
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
			ShouldIgnoreError: shouldIgnoreError1Legacy,
		},
		{
			Func:           hydrate3,
			MaxConcurrency: 3,
			RetryConfig:    &RetryConfig{ShouldRetryError: shouldRetryError1Legacy},
		},
		{
			Func:              hydrate4,
			MaxConcurrency:    4,
			ShouldIgnoreError: shouldIgnoreError2Legacy,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryError2Legacy},
		},
	},
}

var getHydrateConfigTestTableNoDefaultsLegacy = &Table{
	Name:   "test",
	Plugin: getHydrateConfigTestPluginLegacy,
	HydrateConfig: []HydrateConfig{
		{
			Func:           hydrate1,
			MaxConcurrency: 1,
			Depends:        []HydrateFunc{hydrate2, hydrate3},
		},
		{
			Func:              hydrate2,
			MaxConcurrency:    2,
			ShouldIgnoreError: shouldIgnoreError1Legacy,
		},
		{
			Func:           hydrate3,
			MaxConcurrency: 3,
			RetryConfig:    &RetryConfig{ShouldRetryError: shouldRetryError1Legacy},
		},
	},
}

// functions
func shouldIgnoreErrorTableDefault(context.Context, *QueryData, *HydrateData, error) bool {
	return true
}
func shouldIgnoreErrorPluginDefault(context.Context, *QueryData, *HydrateData, error) bool {
	return true
}
func shouldIgnoreError1(context.Context, *QueryData, *HydrateData, error) bool {
	return true
}
func shouldIgnoreError2(context.Context, *QueryData, *HydrateData, error) bool {
	return true
}
func shouldRetryErrorTableDefault(context.Context, *QueryData, *HydrateData, error) bool {
	return true
}
func shouldRetryErrorPluginDefault(context.Context, *QueryData, *HydrateData, error) bool {
	return true
}
func shouldRetryError1(context.Context, *QueryData, *HydrateData, error) bool {
	return true
}
func shouldRetryError2(context.Context, *QueryData, *HydrateData, error) bool {
	return true
}

// legacy functions
func shouldIgnoreErrorTableDefaultLegacy(err error) bool {
	return true
}
func shouldIgnoreErrorPluginDefaultLegacy(err error) bool {
	return true
}
func shouldIgnoreError1Legacy(error) bool {
	return true
}
func shouldIgnoreError2Legacy(error) bool {
	return true
}
func shouldRetryErrorTableDefaultLegacy(error) bool {
	return true
}
func shouldRetryErrorPluginDefaultLegacy(error) bool {
	return true
}
func shouldRetryError1Legacy(error) bool {
	return true
}
func shouldRetryError2Legacy(error) bool {
	return true
}

var testCasesGetHydrateConfig = map[string]getHydrateConfigTest{
	"tables default retry and should ignore": {
		table:    getHydrateConfigTestTableWithDefaults,
		funcName: "hydrate1",

		expected: &HydrateConfig{
			Func:           hydrate1,
			MaxConcurrency: 1,
			RetryConfig:    &RetryConfig{ShouldRetryErrorFunc: shouldRetryErrorTableDefault},
			IgnoreConfig:   &IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreErrorTableDefault},
			Depends:        []HydrateFunc{hydrate2, hydrate3},
		},
	},
	"table default retry": {
		table:    getHydrateConfigTestTableWithDefaults,
		funcName: "hydrate2",

		expected: &HydrateConfig{
			Func:           hydrate2,
			MaxConcurrency: 2,
			RetryConfig:    &RetryConfig{ShouldRetryErrorFunc: shouldRetryErrorTableDefault},
			IgnoreConfig:   &IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreError1},
		},
	},
	"tables default should ignore": {
		table:    getHydrateConfigTestTableWithDefaults,
		funcName: "hydrate3",

		expected: &HydrateConfig{
			Func:           hydrate3,
			MaxConcurrency: 3,
			RetryConfig:    &RetryConfig{ShouldRetryErrorFunc: shouldRetryError1},
			IgnoreConfig:   &IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreErrorTableDefault},
		},
	},
	"plugin default retry and should ignore": {
		table:    getHydrateConfigTestTableNoDefaults,
		funcName: "hydrate1",

		expected: &HydrateConfig{
			Func:           hydrate1,
			MaxConcurrency: 1,
			RetryConfig:    &RetryConfig{ShouldRetryErrorFunc: shouldRetryErrorPluginDefault},
			IgnoreConfig:   &IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreErrorPluginDefault},
			Depends:        []HydrateFunc{hydrate2, hydrate3},
		},
	},
	"plugin default retry": {
		table:    getHydrateConfigTestTableNoDefaults,
		funcName: "hydrate2",

		expected: &HydrateConfig{
			Func:           hydrate2,
			MaxConcurrency: 2,
			RetryConfig:    &RetryConfig{ShouldRetryErrorFunc: shouldRetryErrorPluginDefault},
			IgnoreConfig:   &IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreError1},
		},
	},
	"plugin default should ignore": {
		table:    getHydrateConfigTestTableNoDefaults,
		funcName: "hydrate3",

		expected: &HydrateConfig{
			Func:           hydrate3,
			MaxConcurrency: 3,
			RetryConfig:    &RetryConfig{ShouldRetryErrorFunc: shouldRetryError1},
			IgnoreConfig:   &IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreErrorPluginDefault},
		},
	},
	"no defaults": {
		table:    getHydrateConfigTestTableWithDefaults,
		funcName: "hydrate4",

		expected: &HydrateConfig{
			Func:           hydrate4,
			MaxConcurrency: 4,
			RetryConfig:    &RetryConfig{ShouldRetryErrorFunc: shouldRetryError2},
			IgnoreConfig:   &IgnoreConfig{ShouldIgnoreErrorFunc: shouldIgnoreError2},
		},
	},

	"legacy tables default retry and should ignore": {
		table:    getHydrateConfigTestTableWithDefaultsLegacy,
		funcName: "hydrate1",

		expected: &HydrateConfig{
			Func:              hydrate1,
			MaxConcurrency:    1,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryErrorTableDefaultLegacy},
			ShouldIgnoreError: shouldIgnoreErrorTableDefaultLegacy,
			IgnoreConfig:      &IgnoreConfig{ShouldIgnoreError: shouldIgnoreErrorTableDefaultLegacy},
			Depends:           []HydrateFunc{hydrate2, hydrate3},
		},
	},
	"legacy table default retry": {
		table:    getHydrateConfigTestTableWithDefaultsLegacy,
		funcName: "hydrate2",

		expected: &HydrateConfig{
			Func:              hydrate2,
			MaxConcurrency:    2,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryErrorTableDefaultLegacy},
			ShouldIgnoreError: shouldIgnoreError1Legacy,
			IgnoreConfig:      &IgnoreConfig{ShouldIgnoreError: shouldIgnoreError1Legacy},
		},
	},
	"legacy tables default should ignore": {
		table:    getHydrateConfigTestTableWithDefaultsLegacy,
		funcName: "hydrate3",

		expected: &HydrateConfig{
			Func:              hydrate3,
			MaxConcurrency:    3,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryError1Legacy},
			ShouldIgnoreError: shouldIgnoreErrorTableDefaultLegacy,
			IgnoreConfig:      &IgnoreConfig{ShouldIgnoreError: shouldIgnoreErrorTableDefaultLegacy},
		},
	},
	"legacy plugin default retry and should ignore": {
		table:    getHydrateConfigTestTableNoDefaultsLegacy,
		funcName: "hydrate1",

		expected: &HydrateConfig{
			Func:              hydrate1,
			MaxConcurrency:    1,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryErrorPluginDefaultLegacy},
			ShouldIgnoreError: shouldIgnoreErrorPluginDefaultLegacy,
			IgnoreConfig:      &IgnoreConfig{ShouldIgnoreError: shouldIgnoreErrorPluginDefaultLegacy},
			Depends:           []HydrateFunc{hydrate2, hydrate3},
		},
	},
	"legacy plugin default retry": {
		table:    getHydrateConfigTestTableNoDefaultsLegacy,
		funcName: "hydrate2",

		expected: &HydrateConfig{
			Func:              hydrate2,
			MaxConcurrency:    2,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryErrorPluginDefaultLegacy},
			ShouldIgnoreError: shouldIgnoreError1Legacy,
			IgnoreConfig:      &IgnoreConfig{ShouldIgnoreError: shouldIgnoreError1Legacy},
		},
	},
	"legacy plugin default should ignore": {
		table:    getHydrateConfigTestTableNoDefaultsLegacy,
		funcName: "hydrate3",

		expected: &HydrateConfig{
			Func:              hydrate3,
			MaxConcurrency:    3,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryError1Legacy},
			ShouldIgnoreError: shouldIgnoreErrorPluginDefaultLegacy,
			IgnoreConfig:      &IgnoreConfig{ShouldIgnoreError: shouldIgnoreErrorPluginDefaultLegacy},
		},
	},
	"legacy no defaults": {
		table:    getHydrateConfigTestTableWithDefaultsLegacy,
		funcName: "hydrate4",

		expected: &HydrateConfig{
			Func:              hydrate4,
			MaxConcurrency:    4,
			RetryConfig:       &RetryConfig{ShouldRetryError: shouldRetryError2Legacy},
			ShouldIgnoreError: shouldIgnoreError2Legacy,
			IgnoreConfig:      &IgnoreConfig{ShouldIgnoreError: shouldIgnoreError2Legacy},
		},
	},
}

func TestGetHydrateConfig(t *testing.T) {
	logger := hclog.NewNullLogger()
	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))

	for name, test := range testCasesGetHydrateConfig {
		// initialise the expected
		test.expected.initialise(test.table)
		// initialise the plugin and table
		test.table.Plugin.initialise(logger)
		test.table.initialise(test.table.Plugin)

		result := test.table.hydrateConfigMap[test.funcName]
		actualString := result.String()
		expectedString := test.expected.String()
		if expectedString != actualString {
			t.Errorf("Test: '%s'' FAILED : expected: \n\n%v\n\ngot: \n\n%v", name, expectedString, actualString)
		}
	}
}

// validate table

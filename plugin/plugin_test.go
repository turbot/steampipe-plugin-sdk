package plugin

import (
	"context"
	"testing"

	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
)

type validateTest struct {
	plugin   Plugin
	expected string
}

// test hydrate functions
func listHydrate(context.Context, *QueryData, *HydrateData) (interface{}, error) {
	return nil, nil
}
func getHydrate(context.Context, *QueryData, *HydrateData) (interface{}, error) {
	return nil, nil
}
func itemFromKey(context.Context, *QueryData, *HydrateData) (interface{}, error) {
	return nil, nil
}
func isNotFound(error) bool { return false }
func hydrate1(context.Context, *QueryData, *HydrateData) (interface{}, error) {
	return nil, nil
}
func hydrate2(context.Context, *QueryData, *HydrateData) (interface{}, error) {
	return nil, nil
}
func hydrate3(context.Context, *QueryData, *HydrateData) (interface{}, error) {
	return nil, nil
}
func hydrate4(context.Context, *QueryData, *HydrateData) (interface{}, error) {
	return nil, nil
}

var testCasesValidate = map[string]validateTest{
	"valid": {
		plugin: Plugin{
			Name: "plugin",
			TableMap: map[string]*Table{
				"table": {
					Name: "table",
					Columns: []*Column{
						{
							Name: "name",
							Type: proto.ColumnType_STRING,
						},
						{
							Name:    "c1",
							Type:    proto.ColumnType_STRING,
							Hydrate: hydrate1,
						},
						{
							Name:    "c2",
							Type:    proto.ColumnType_STRING,
							Hydrate: hydrate2,
						},
					},
					List: &ListConfig{
						Hydrate: listHydrate,
					},
					Get: &GetConfig{
						KeyColumns:        SingleColumn("name"),
						Hydrate:           getHydrate,
						ShouldIgnoreError: isNotFound,
					},
					HydrateDependencies: []HydrateDependencies{{Func: hydrate2, Depends: []HydrateFunc{hydrate1}}},
				},
			},
			RequiredColumns: []*Column{{Name: "name", Type: proto.ColumnType_STRING}},
		},
		expected: "",
	},
	"circular dep": {
		plugin: Plugin{
			Name: "plugin",
			TableMap: map[string]*Table{
				"table": {
					Name: "table",
					Columns: []*Column{
						{
							Name: "name",
							Type: proto.ColumnType_STRING,
						},
						{
							Name:    "c1",
							Type:    proto.ColumnType_STRING,
							Hydrate: hydrate1,
						},
						{
							Name:    "c2",
							Type:    proto.ColumnType_STRING,
							Hydrate: hydrate2,
						},
					},
					List: &ListConfig{
						Hydrate: listHydrate,
					},
					Get: &GetConfig{
						KeyColumns:        SingleColumn("name"),
						Hydrate:           getHydrate,
						ShouldIgnoreError: isNotFound,
					},
					HydrateDependencies: []HydrateDependencies{
						{Func: hydrate1, Depends: []HydrateFunc{hydrate2}},
						{Func: hydrate2, Depends: []HydrateFunc{hydrate1}},
					},
				},
			},
			RequiredColumns: []*Column{{Name: "name", Type: proto.ColumnType_STRING}},
		},
		expected: "Hydration dependencies contains cycle: : hydrate1 -> hydrate2 -> hydrate1",
	},
	"no get key": {
		plugin: Plugin{
			Name: "plugin",
			TableMap: map[string]*Table{
				"table": {
					Name: "table",
					Columns: []*Column{
						{
							Name: "name",
							Type: proto.ColumnType_STRING,
						},
						{
							Name:    "c1",
							Type:    proto.ColumnType_STRING,
							Hydrate: hydrate1,
						},
					},
					List: &ListConfig{
						Hydrate: listHydrate,
					},
					Get: &GetConfig{
						Hydrate:           getHydrate,
						ShouldIgnoreError: isNotFound,
					},
					HydrateDependencies: []HydrateDependencies{{Func: hydrate1, Depends: []HydrateFunc{getHydrate}}},
				},
			},
			RequiredColumns: []*Column{{Name: "name", Type: proto.ColumnType_STRING}},
		},
		expected: "table 'table' GetConfig does not specify a key",
	},
	"no get hydrate": {
		plugin: Plugin{
			Name: "plugin",
			TableMap: map[string]*Table{
				"table": {
					Name: "table",
					Columns: []*Column{
						{
							Name: "name",
							Type: proto.ColumnType_STRING,
						},
						{
							Name:    "c1",
							Type:    proto.ColumnType_STRING,
							Hydrate: hydrate1,
						},
					},
					List: &ListConfig{
						Hydrate: listHydrate,
					},
					Get: &GetConfig{
						KeyColumns:        SingleColumn("name"),
						ShouldIgnoreError: isNotFound,
					},
					HydrateDependencies: []HydrateDependencies{{Func: hydrate1, Depends: []HydrateFunc{getHydrate}}},
				},
			},
			RequiredColumns: []*Column{{Name: "name", Type: proto.ColumnType_STRING}},
		},
		expected: "table 'table' GetConfig does not specify a hydrate function",
	},
	"no list hydrate": {
		plugin: Plugin{
			Name: "plugin",
			TableMap: map[string]*Table{
				"table": {
					Name: "table",
					Columns: []*Column{
						{
							Name: "name",
							Type: proto.ColumnType_STRING,
						},
						{
							Name:    "c1",
							Type:    proto.ColumnType_STRING,
							Hydrate: hydrate1,
						},
					},
					List: &ListConfig{},
					Get: &GetConfig{
						KeyColumns:        SingleColumn("name"),
						Hydrate:           getHydrate,
						ShouldIgnoreError: isNotFound,
					},
					HydrateDependencies: []HydrateDependencies{{Func: hydrate1, Depends: []HydrateFunc{getHydrate}}},
				},
			},
			RequiredColumns: []*Column{{Name: "name", Type: proto.ColumnType_STRING}},
		},
		expected: "table 'table' ListConfig does not specify a hydrate function",
	},
	"no list or get config": {
		plugin: Plugin{
			Name: "plugin",
			TableMap: map[string]*Table{
				"table": {
					Name: "table",
					Columns: []*Column{
						{
							Name: "name",
							Type: proto.ColumnType_STRING,
						},
						{
							Name:    "c1",
							Type:    proto.ColumnType_STRING,
							Hydrate: hydrate1,
						},
					},
					HydrateDependencies: []HydrateDependencies{{Func: hydrate1, Depends: []HydrateFunc{getHydrate}}},
				},
			},
			RequiredColumns: []*Column{{Name: "name", Type: proto.ColumnType_STRING}},
		},
		expected: "table 'table' does not have either GetConfig or ListConfig - one of these must be provided",
	},
	"required column wrong type": {
		plugin: Plugin{
			Name: "plugin",
			TableMap: map[string]*Table{
				"table": {
					Name: "table",
					Columns: []*Column{
						{
							Name: "name",
							Type: proto.ColumnType_INT,
						},
						{
							Name:    "c1",
							Type:    proto.ColumnType_STRING,
							Hydrate: hydrate1,
						},
					},
					List: &ListConfig{
						Hydrate: listHydrate,
					},
					Get: &GetConfig{
						KeyColumns:        SingleColumn("name"),
						Hydrate:           getHydrate,
						ShouldIgnoreError: isNotFound,
					},
					HydrateDependencies: []HydrateDependencies{{Func: hydrate1, Depends: []HydrateFunc{getHydrate}}},
				},
			},
			RequiredColumns: []*Column{{Name: "name", Type: proto.ColumnType_STRING}},
		},
		expected: "table 'table' required column 'name' should be type 'ColumnType_STRING' but is type 'ColumnType_INT'",
	},
	"missing required column": {
		plugin: Plugin{
			Name: "plugin",
			TableMap: map[string]*Table{
				"table": {
					Name: "table",
					Columns: []*Column{
						{
							Name: "name",
							Type: proto.ColumnType_STRING,
						},
						{
							Name:    "c1",
							Type:    proto.ColumnType_STRING,
							Hydrate: hydrate1,
						},
					},
					List: &ListConfig{
						Hydrate: listHydrate,
					},
					Get: &GetConfig{
						KeyColumns:        SingleColumn("name"),
						Hydrate:           getHydrate,
						ShouldIgnoreError: isNotFound,
					},
					HydrateDependencies: []HydrateDependencies{{Func: hydrate1, Depends: []HydrateFunc{getHydrate}}},
				},
			},
			RequiredColumns: []*Column{{Name: "missing", Type: proto.ColumnType_STRING}},
		},
		expected: "table 'table' does not implement required column 'missing'",
	},
	"missing get key": {
		plugin: Plugin{
			Name: "plugin",
			TableMap: map[string]*Table{
				"table": {
					Name: "table",
					Columns: []*Column{
						{
							Name: "name",
							Type: proto.ColumnType_STRING,
						},
						{
							Name:    "c1",
							Type:    proto.ColumnType_STRING,
							Hydrate: hydrate1,
						},
					},
					List: &ListConfig{
						Hydrate: listHydrate,
					},
					Get: &GetConfig{
						Hydrate:           getHydrate,
						ShouldIgnoreError: isNotFound,
					},
					HydrateDependencies: []HydrateDependencies{{Func: hydrate1, Depends: []HydrateFunc{getHydrate}}},
				},
			},
			RequiredColumns: []*Column{{Name: "name", Type: proto.ColumnType_STRING}},
		},
		expected: "table 'table' GetConfig does not specify a key",
	},
}

func TestValidate(t *testing.T) {
	for name, test := range testCasesValidate {
		validationErrors := test.plugin.Validate()

		if test.expected != validationErrors {
			t.Errorf("Test: '%s'' FAILED. \nExpected: '%s' \nGot: '%s'", name, test.expected, validationErrors)
		}
	}
}

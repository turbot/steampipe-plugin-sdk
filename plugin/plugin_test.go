package plugin

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
	"log"
	"strings"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

type validateTest struct {
	plugin   Plugin
	expected []string
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
		expected: []string{""},
	},
	"invalid limiter name": {
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
			RateLimiters: []*rate_limiter.Definition{
				{
					Name:           "1invalid",
					MaxConcurrency: 10,
				},
			},
		},

		expected: []string{"invalid rate limiter name '1invalid' - names can contain letters, digits, underscores (_), and hyphens (-), and cannot start with a digit"},
	},
	"get with hydrate dependency": {
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
					HydrateDependencies: []HydrateDependencies{{Func: getHydrate, Depends: []HydrateFunc{hydrate1}}},
				},
			},
			RequiredColumns: []*Column{{Name: "name", Type: proto.ColumnType_STRING}},
		},
		expected: []string{"table 'table' Get hydrate function 'getHydrate' has 1 dependency - Get hydrate functions cannot have dependencies"},
	},
	"get with explicit hydrate config": {
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
					HydrateConfig: []HydrateConfig{{Func: getHydrate, Depends: []HydrateFunc{hydrate1}}},
				},
			},
			RequiredColumns: []*Column{{Name: "name", Type: proto.ColumnType_STRING}},
		},
		expected: []string{"table 'table' Get hydrate function 'getHydrate' defines dependendencies in its `HydrateConfig`"},
	},
	"list with hydrate dependency": {
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
					HydrateDependencies: []HydrateDependencies{{Func: listHydrate, Depends: []HydrateFunc{hydrate1}}},
				},
			},
			RequiredColumns: []*Column{{Name: "name", Type: proto.ColumnType_STRING}},
		},
		expected: []string{"table 'table' List hydrate function 'listHydrate' has 1 dependency - List hydrate functions cannot have dependencies"},
	},
	"list with explicit hydrate config": {
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
					HydrateConfig: []HydrateConfig{{Func: listHydrate, Depends: []HydrateFunc{hydrate1}}},
				},
			},
			RequiredColumns: []*Column{{Name: "name", Type: proto.ColumnType_STRING}},
		},
		expected: []string{"table 'table' List hydrate function 'listHydrate' defines dependencies in its `HydrateConfig`"},
	},
	// non deterministic - skip
	//"circular dep": {
	//	plugin: Plugin{
	//		Name: "plugin",
	//		TableMap: map[string]*Table{
	//			"table": {
	//				Name: "table",
	//				Columns: []*Column{
	//					{
	//						Name: "name",
	//						Type: proto.ColumnType_STRING,
	//					},
	//					{
	//						Name:    "c1",
	//						Type:    proto.ColumnType_STRING,
	//						Hydrate: hydrate1,
	//					},
	//					{
	//						Name:    "c2",
	//						Type:    proto.ColumnType_STRING,
	//						Hydrate: hydrate2,
	//					},
	//				},
	//				List: &ListConfig{
	//					Hydrate: listHydrate,
	//				},
	//				Get: &GetConfig{
	//					KeyColumns:        SingleColumn("name"),
	//					Hydrate:           getHydrate,
	//					ShouldIgnoreError: isNotFound,
	//				},
	//				HydrateDependencies: []HydrateDependencies{
	//					{Func: hydrate1, Depends: []HydrateFunc{hydrate2}},
	//					{Func: hydrate2, Depends: []HydrateFunc{hydrate1}},
	//				},
	//			},
	//		},
	//		RequiredColumns: []*Column{{Name: "name", Type: proto.ColumnType_STRING}},
	//	},
	//	expected: []string{"Hydration dependencies contains cycle: : hydrate1 -> hydrate2 -> hydrate1",
	//},
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
		expected: []string{"table 'table' GetConfig does not specify a KeyColumn"},
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
		expected: []string{"table 'table' GetConfig does not specify a hydrate function\ntable 'table' HydrateConfig does not specify a hydrate function"},
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
		expected: []string{"table 'table' ListConfig does not specify a hydrate function"},
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
		expected: []string{"table 'table' does not have either GetConfig or ListConfig - one of these must be provided"},
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
		expected: []string{"table 'table' required column 'name' should be type 'ColumnType_STRING' but is type 'ColumnType_INT'"},
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
		expected: []string{"table 'table' does not implement required column 'missing'"},
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
		expected: []string{"table 'table' GetConfig does not specify a KeyColumn"},
	},
}

func TestValidate(t *testing.T) {
	for name, test := range testCasesValidate {
		logger := hclog.NewNullLogger()
		log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))

		test.plugin.initialise(logger)
		test.plugin.initialiseTables(context.Background(), &Connection{Name: "test"})

		_, validationErrors := test.plugin.validate(test.plugin.TableMap)

		if strings.Join(test.expected, "\n") != strings.Join(validationErrors, "\n") {
			t.Errorf("Test: '%s'' FAILED. \nexpected: []string{'%s' \nGot: '%s'  ", name, test.expected, validationErrors)
		}
	}
}

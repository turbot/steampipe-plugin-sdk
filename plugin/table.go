package plugin

import (
	"context"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v2/plugin/transform"
)

type MatrixItemFunc func(context.Context, *Connection) []map[string]interface{}
type ErrorPredicate func(error) bool

// Table represents a plugin table
type Table struct {
	Name string
	// table description
	Description string
	// column definitions
	Columns          []*Column
	List             *ListConfig
	Get              *GetConfig
	GetMatrixItem    MatrixItemFunc
	DefaultTransform *transform.ColumnTransforms
	// the parent plugin object
	Plugin *Plugin
	// definitions of dependencies between hydrate functions
	HydrateDependencies []HydrateDependencies
	HydrateConfig       []HydrateConfig
	// map of hydrate function name to columns it provides
	hydrateColumnMap map[string][]string
}

type GetConfig struct {
	// key or keys which are used to uniquely identify rows - used to determine whether  a query is a 'get' call
	KeyColumns KeyColumnSlice
	// the hydrate function which is called first when performing a 'get' call.
	// if this returns 'not found', no further hydrate functions are called
	Hydrate HydrateFunc
	// a function which will return whenther to ignore a given error
	ShouldIgnoreError ErrorPredicate
	RetryConfig       *RetryConfig
}

type ListConfig struct {
	KeyColumns KeyColumnSlice
	// the list function, this should stream the list results back using the QueryData object, and return nil
	Hydrate HydrateFunc
	// the parent list function - if we list items with a parent-child relationship, this will list the parent items
	ParentHydrate     HydrateFunc
	ShouldIgnoreError ErrorPredicate
	RetryConfig       *RetryConfig
}

// build a list of required hydrate function calls which must be executed, based on the columns which have been requested
// NOTE: 'get' and 'list' calls are hydration functions, but they must be omitted from this list as they are called
// first. BEFORE the other hydration functions
// NOTE2: this function also populates the resolvedHydrateName for each column (used to retrieve column values),
// and the hydrateColumnMap (used to determine which columns to return)
func (t *Table) requiredHydrateCalls(colsUsed []string, fetchType fetchType) []*HydrateCall {
	// what is the name of the fetch call (i.e. the get/list call)
	fetchFunc := t.getFetchFunc(fetchType)
	fetchCallName := helpers.GetFunctionName(fetchFunc)

	// initialise hydrateColumnMap
	t.hydrateColumnMap = make(map[string][]string)
	requiredCallBuilder := newRequiredHydrateCallBuilder(t, fetchCallName)

	// populate a map keyed by function name to ensure we only store each hydrate function once
	for _, column := range t.Columns {
		// see if this column specifies a hydrate function

		var hydrateName string
		if hydrateFunc := column.Hydrate; hydrateFunc == nil {
			// so there is NO hydrate call registered for the column
			// the column is provided by the fetch call
			// do not add to map of hydrate functions as the fetch call will always be called
			hydrateFunc = fetchFunc
			hydrateName = fetchCallName
		} else {
			// there is a hydrate call registered
			hydrateName = helpers.GetFunctionName(hydrateFunc)
			// if this column was requested in query, add the hydrate call to required calls
			if helpers.StringSliceContains(colsUsed, column.Name) {
				requiredCallBuilder.Add(hydrateFunc)
			}
		}

		// now update hydrateColumnMap
		t.hydrateColumnMap[hydrateName] = append(t.hydrateColumnMap[hydrateName], column.Name)
	}
	return requiredCallBuilder.Get()
}

func (t *Table) getFetchFunc(fetchType fetchType) HydrateFunc {

	if fetchType == fetchTypeList {
		return t.List.Hydrate
	}
	return t.Get.Hydrate

}

func (t *Table) getHydrateDependencies(hydrateFuncName string) []HydrateFunc {
	for _, d := range t.HydrateDependencies {
		if helpers.GetFunctionName(d.Func) == hydrateFuncName {
			return d.Depends
		}
	}
	return []HydrateFunc{}
}

func (t *Table) getHydrateConfig(hydrateFuncName string) *HydrateConfig {
	config := &HydrateConfig{}
	// if a hydrate config is defined see whether this call exists in it
	for _, d := range t.HydrateConfig {
		if helpers.GetFunctionName(d.Func) == hydrateFuncName {
			config = &d
		}
	}
	if config.RetryConfig == nil {
		config.RetryConfig = t.Plugin.DefaultRetryConfig
	}
	// if no hydrate dependencies are specified in the hydrate config, check the deprecated "HydrateDependencies" property
	if config.Depends == nil {
		config.Depends = t.getHydrateDependencies(hydrateFuncName)
	}

	return config
}

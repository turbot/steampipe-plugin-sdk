package plugin

import (
	"context"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v2/plugin/transform"
)

type MatrixItemFunc func(context.Context, *Connection) []map[string]interface{}
type ErrorPredicate func(error) bool

// Table is a struct representing a plugin table.
// It defines the table columns, the function used to list table results (List)
// as well as (optionally) the function used to retrieve a single result by key (Get)
// and additional the functions required to fetch specific columns (HydrateConfig).
type Table struct {
	Name string
	// table description
	Description string
	// column definitions
	Columns []*Column
	// the function used to list table rows
	List *ListConfig
	// the function used to efficiently retrieve a row by id
	Get *GetConfig
	// the function used when retrieving data for multiple 'matrix items', e.g. regions
	GetMatrixItem MatrixItemFunc
	// default transform applied to all columns
	DefaultTransform *transform.ColumnTransforms
	// function controlling default error handling behaviour
	DefaultShouldIgnoreError ErrorPredicate
	DefaultRetryConfig       *RetryConfig
	// the parent plugin object
	Plugin *Plugin
	// Deprecated: used HydrateConfig
	HydrateDependencies []HydrateDependencies
	// Config for any required hydrate functions, including dependencies between hydrate functions,
	// error handling and concurrency behaviour
	HydrateConfig []HydrateConfig

	// map of hydrate function name to columns it provides
	hydrateColumnMap map[string][]string
}

// GetConfig is a struct used to define the configuration of the table 'Get' function.
// This is the function used to retrieve a single row by id
// The config defines the function, the columns which may be used as id (KeyColumns), and the error handling behaviour
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

// ListConfig is a struct used to define the configuration of the table 'List' function.
// This is the function used to retrieve rows of sata
// The config defines the function, the columns which may be used to optimise the fetch (KeyColumns),
// and the error handling behaviour
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

// search through HydrateConfig and HydrateDependencies finding a function with the given name
// if found return its dependencies
func (t *Table) getHydrateDependencies(hydrateFuncName string) []HydrateFunc {
	for _, d := range t.HydrateConfig {
		if helpers.GetFunctionName(d.Func) == hydrateFuncName {
			return d.Depends
		}
	}
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
			break
		}
	}
	// now use default values if needed
	if config.RetryConfig == nil {
		if t.DefaultRetryConfig != nil {
			config.RetryConfig = t.DefaultRetryConfig
		} else {
			config.RetryConfig = t.Plugin.DefaultRetryConfig
		}
	}
	if config.ShouldIgnoreError == nil {
		if t.DefaultShouldIgnoreError != nil {
			config.ShouldIgnoreError = t.DefaultShouldIgnoreError
		} else {
			config.ShouldIgnoreError = t.Plugin.DefaultShouldIgnoreError
		}
	}

	// if no hydrate dependencies are specified in the hydrate config, check the deprecated "HydrateDependencies" property
	if config.Depends == nil {
		config.Depends = t.getHydrateDependencies(hydrateFuncName)
	}

	return config
}

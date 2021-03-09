package plugin

import (
	"context"
	"log"

	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"

	"github.com/turbot/go-kit/helpers"
)

type MatrixItemFunc func(context.Context, *Connection) []map[string]interface{}

// Table :: struct representing a plugin table
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
}

type GetConfig struct {
	// key or keys which are used to uniquely identify rows - used to determine whether  a query is a 'get' call
	KeyColumns  *KeyColumnSet
	ItemFromKey HydrateFunc
	// the hydrate function which is called first when performing a 'get' call.
	// if this returns 'not found', no further hydrate functions are called
	Hydrate HydrateFunc
	// a function which will return whenther to ignore a given error
	ShouldIgnoreError ErrorPredicate
	RetryConfig       *RetryConfig
}

type ListConfig struct {
	KeyColumns *KeyColumnSet
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
func (t *Table) requiredHydrateCalls(colsUsed []string, fetchType fetchType) []*HydrateCall {
	log.Printf("[TRACE] requiredHydrateCalls, fetchType %s colsUsed %v\n", fetchType, colsUsed)

	// what is the name of the fetch call (i.e. the get/list call)
	var fetchCallName string
	if fetchType == fetchTypeList {
		fetchCallName = helpers.GetFunctionName(t.List.Hydrate)
	} else {
		fetchCallName = helpers.GetFunctionName(t.Get.Hydrate)
	}

	requiredCallBuilder := newRequiredHydrateCallBuilder(t, fetchCallName)

	// populate a map keyed by function name to ensure we only store each hydrate function once
	for _, column := range t.Columns {
		if helpers.StringSliceContains(colsUsed, column.Name) {

			// see if this column specifies a hydrate function
			hydrateFunc := column.Hydrate
			if hydrateFunc != nil {
				log.Printf("[TRACE] column '%s'  - resolved hydration function '%s'\n", column.Name, column.resolvedHydrateName)
				hydrateName := helpers.GetFunctionName(hydrateFunc)
				column.resolvedHydrateName = hydrateName
				requiredCallBuilder.Add(hydrateFunc)
			} else {
				// so there is no hydrate call registered for the column - the resolvedHydrateName is the fetch call
				log.Printf("[TRACE] column '%s'  - resolved hydration function is the fetch call: '%s'\n", column.Name, column.resolvedHydrateName)

				column.resolvedHydrateName = fetchCallName
				// do not add to map of hydrate functions as the fetch call will always be called
			}
		}
	}

	return requiredCallBuilder.Get()
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

// return the column matching the given nam
func (t *Table) columnForName(columnName string) (*Column, bool) {
	for _, c := range t.Columns {
		if c.Name == columnName {
			return c, true
		}
	}
	return nil, false
}

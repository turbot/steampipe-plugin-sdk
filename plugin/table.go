package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
	"log"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

/*
TableCacheOptions provides a mechanism to disable caching for a specific table.

It is useful in cases where the table returns a huge volume of data cheaply.

Use TableCacheOptions to override the .cache off property of the CLI.
*/
type TableCacheOptions struct {
	Enabled bool
}

/*
|
Table defines the properties of a plugin table:

  - The columns that are returned: [plugin.Table.Columns].

  - How to fetch all rows in the table: [plugin.Table.List].

  - How to fetch a single row by key: [plugin.Table.Get].

  - Additional configuration for a column hydrate function: [plugin.Table.HydrateConfig].

  - Function used to retrieve data for multiple matrix items: [plugin.Table.GetMatrixItemFunc].

  - The table default [error_handling] behaviour.
*/
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
	// deprecated
	// the function used when retrieving data for multiple 'matrix items', e.g. regions
	GetMatrixItem     MatrixItemFunc
	GetMatrixItemFunc MatrixItemMapFunc
	// default transform applied to all columns
	DefaultTransform *transform.ColumnTransforms
	// function controlling default error handling behaviour
	DefaultIgnoreConfig *IgnoreConfig
	DefaultRetryConfig  *RetryConfig
	// the parent plugin object
	Plugin *Plugin
	// Deprecated: used HydrateConfig
	HydrateDependencies []HydrateDependencies
	// Config for any required hydrate functions, including dependencies between hydrate functions,
	// error handling and concurrency behaviour
	HydrateConfig []HydrateConfig
	// cache options - allows disabling of cache for this table
	Cache *TableCacheOptions

	// tags used to provide scope values for all child hydrate calls
	// (may be used for more in future)
	Tags map[string]string

	// deprecated - use DefaultIgnoreConfig
	DefaultShouldIgnoreError ErrorPredicate

	// map of hydrate function name to columns it provides
	hydrateConfigMap map[string]*HydrateConfig

	columnNameMap map[string]struct{}
}

func (t *Table) initialise(p *Plugin) {
	log.Printf("[TRACE] initialise table %s", t.Name)

	// store the plugin pointer
	t.Plugin = p

	// create DefaultRetryConfig if needed
	if t.DefaultRetryConfig == nil {
		log.Printf("[TRACE] no DefaultRetryConfig defined - creating empty")
		t.DefaultRetryConfig = &RetryConfig{}
	}

	// create DefaultIgnoreConfig if needed
	if t.DefaultIgnoreConfig == nil {
		log.Printf("[TRACE] no DefaultIgnoreConfig defined - creating empty")
		t.DefaultIgnoreConfig = &IgnoreConfig{}
	}

	// create RateLimit if needed
	if t.Tags == nil {
		t.Tags = make(map[string]string)
	}
	// populate tags with table name
	t.Tags[rate_limiter.RateLimiterScopeTable] = t.Name

	if t.DefaultShouldIgnoreError != nil && t.DefaultIgnoreConfig.ShouldIgnoreError == nil {
		// copy the (deprecated) top level ShouldIgnoreError property into the ignore config
		t.DefaultIgnoreConfig.ShouldIgnoreError = t.DefaultShouldIgnoreError
		log.Printf("[TRACE] legacy DefaultShouldIgnoreError defined - copying into DefaultIgnoreConfig")
	}

	// apply plugin defaults for retry and ignore config
	log.Printf("[TRACE] apply plugin defaults for DefaultRetryConfig")
	t.DefaultRetryConfig.DefaultTo(t.Plugin.DefaultRetryConfig)
	log.Printf("[TRACE] apply plugin defaults for DefaultIgnoreConfig, table %v plugin %v", t.DefaultIgnoreConfig, t.Plugin.DefaultIgnoreConfig)
	t.DefaultIgnoreConfig.DefaultTo(t.Plugin.DefaultIgnoreConfig)

	log.Printf("[TRACE] DefaultRetryConfig: %s", t.DefaultRetryConfig.String())
	log.Printf("[TRACE] DefaultIgnoreConfig: %s", t.DefaultIgnoreConfig.String())

	// get and list configs are similar to hydrate configs but they cannot specify dependencies
	// we initialise them first, then initialise the hydrate configs
	if t.Get != nil {
		log.Printf("[TRACE] t.Get.initialise")
		t.Get.initialise(t)
	}
	if t.List != nil {
		log.Printf("[TRACE] t.List.initialise")
		t.List.initialise(t)
	}

	// HydrateConfig contains explicit config for hydrate functions but there may be other hydrate functions
	// declared for specific columns which do not have config defined
	// build a map of all hydrate functions, with empty config if needed
	// NOTE: this map also includes information from the legacy HydrateDependencies property
	t.initialiseHydrateConfigs()

	t.setColumnNameMap()

	log.Printf("[TRACE] back from initialiseHydrateConfigs")

	log.Printf("[TRACE] initialise table %s COMPLETE", t.Name)
}

func (t *Table) setColumnNameMap() {
	t.columnNameMap = make(map[string]struct{}, len(t.Columns))
	for _, c := range t.Columns {
		t.columnNameMap[c.Name] = struct{}{}
	}
}

// build map of all hydrate configs, and initialise them
func (t *Table) initialiseHydrateConfigs() {
	// first build a map of all hydrate functions
	t.buildHydrateConfigMap()

	//  initialise all hydrate configs in map
	for _, h := range t.hydrateConfigMap {
		h.initialise(t)
	}
}

// build map of all hydrate configs, including those specified in the legacy HydrateDependencies,
// and those mentioned only in column config
func (t *Table) buildHydrateConfigMap() {
	t.hydrateConfigMap = make(map[string]*HydrateConfig)
	for i := range t.HydrateConfig {
		// as we are converting into a pointer, we cannot use the array value direct from the range as
		// this was causing incorrect values - go must be reusing memory addresses for successive items
		h := &t.HydrateConfig[i]
		funcName := helpers.GetFunctionName(h.Func)
		t.hydrateConfigMap[funcName] = h
	}
	// add in hydrate config for all hydrate dependencies declared using legacy property HydrateDependencies
	for _, d := range t.HydrateDependencies {
		hydrateName := helpers.GetFunctionName(d.Func)
		// if there is already a hydrate config, do nothing here
		// (this is a validation error that will be picked up by the validation check later)
		if _, ok := t.hydrateConfigMap[hydrateName]; !ok {
			t.hydrateConfigMap[hydrateName] = &HydrateConfig{Func: d.Func, Depends: d.Depends}
		}
	}
	// NOTE: the get config may be used as a column hydrate function so add this into the map
	if get := t.Get; get != nil {
		hydrateName := helpers.GetFunctionName(get.Hydrate)
		t.hydrateConfigMap[hydrateName] = &HydrateConfig{
			Func:              get.Hydrate,
			IgnoreConfig:      get.IgnoreConfig,
			RetryConfig:       get.RetryConfig,
			Tags:              get.Tags,
			ShouldIgnoreError: get.ShouldIgnoreError,
			MaxConcurrency:    get.MaxConcurrency,
		}
	}

	// now add all hydrate functions with no explicit config
	for _, c := range t.Columns {
		if c.Hydrate == nil {
			continue
		}
		hydrateName := helpers.GetFunctionName(c.Hydrate)
		if _, ok := t.hydrateConfigMap[hydrateName]; !ok {
			t.hydrateConfigMap[hydrateName] = &HydrateConfig{Func: c.Hydrate}
		}
	}
}

func (t *Table) getFetchFunc(fetchType fetchType) HydrateFunc {
	if fetchType == fetchTypeList {
		return t.List.Hydrate
	}
	return t.Get.Hydrate
}

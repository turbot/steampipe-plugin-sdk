package plugin

import (
	"fmt"
	"github.com/stevenle/topsort"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"strings"
)

func (t *Table) validate(name string, requiredColumns []*Column) (validationWarnings, validationErrors []string) {
	validationWarnings, validationErrors = t.validateReservedColumns()

	// does table have a name set?
	if t.Name == "" {
		validationErrors = append(validationErrors, fmt.Sprintf("table with key '%s' in plugin table map does not have a name property set", name))
	}

	// verify all required columns exist
	validationErrors = append(validationErrors, t.validateRequiredColumns(requiredColumns)...)

	// validated list and get config
	// NOTE: this also sets key column require and operators to default value if not specified
	validationErrors = append(validationErrors, t.validateListAndGetConfig()...)

	// verify hydrate dependencies are valid
	// the map entries are strings - ensure they correspond to actual functions
	validationErrors = append(validationErrors, t.validateHydrateDependencies()...)

	validationErrors = append(validationErrors, t.DefaultRetryConfig.validate(t)...)

	validationErrors = append(validationErrors, t.DefaultIgnoreConfig.validate(t)...)

	for _, h := range t.hydrateConfigMap {
		validationErrors = append(validationErrors, h.validate(t)...)
	}

	for _, c := range t.Columns {
		validationErrors = append(validationErrors, c.validate(t)...)
	}
	return validationWarnings, validationErrors
}

func (t *Table) validateRequiredColumns(requiredColumns []*Column) []string {
	var validationErrors []string
	if len(requiredColumns) > 0 {
		for _, requiredColumn := range requiredColumns {
			// get column def from this t
			c := t.getColumn(requiredColumn.Name)

			if c == nil {
				validationErrors = append(validationErrors, fmt.Sprintf("table '%s' does not implement required column '%s'", t.Name, requiredColumn.Name))
			} else if c.Type != requiredColumn.Type {
				validationErrors = append(validationErrors, fmt.Sprintf("table '%s' required column '%s' should be type '%s' but is type '%s'", t.Name, requiredColumn.Name, columnTypeToString(requiredColumn.Type), columnTypeToString(c.Type)))
			}
		}
	}
	return validationErrors
}

func columnTypeToString(columnType proto.ColumnType) string {
	switch columnType {
	case proto.ColumnType_BOOL:
		return "ColumnType_BOOL"
	case proto.ColumnType_INT:
		return "ColumnType_INT"
	case proto.ColumnType_DOUBLE:
		return "ColumnType_DOUBLE"
	case proto.ColumnType_STRING:
		return "ColumnType_STRING"
	case proto.ColumnType_JSON:
		return "ColumnType_BOOL"
	case proto.ColumnType_DATETIME:
		return "ColumnType_DATETIME"
	case proto.ColumnType_IPADDR:
		return "ColumnType_IPADDR"
	case proto.ColumnType_CIDR:
		return "ColumnType_CIDR"
	case proto.ColumnType_INET:
		return "ColumnType_INET"
	case proto.ColumnType_TIMESTAMP:
		return "ColumnType_TIMESTAMP"
	case proto.ColumnType_LTREE:
		return "ColumnType_LTREE"
	default:
		return fmt.Sprintf("Unknown column type: %v", columnType)
	}
}

// validate list and get config
// NOTE: this initialises key column properties to their defaults
func (t *Table) validateListAndGetConfig() []string {
	var validationErrors []string
	// either get or list must be defined
	if t.List == nil && t.Get == nil {
		validationErrors = append(validationErrors, fmt.Sprintf("table '%s' does not have either GetConfig or ListConfig - one of these must be provided", t.Name))
	}

	if t.Get != nil {
		validationErrors = append(validationErrors, t.Get.Validate(t)...)
	}
	if t.List != nil {
		validationErrors = append(validationErrors, t.List.Validate(t)...)
	}

	// verify any key columns defined for GET only use '=' operators
	// also set key column require and operators to default value if not specified
	validationErrors = append(validationErrors, t.validateKeyColumns()...)

	return validationErrors
}

func (t *Table) validateHydrateDependencies() []string {
	// only 1 of HydrateDependencies and HydrateConfig) may be set
	if len(t.HydrateDependencies) != 0 && len(t.HydrateConfig) != 0 {
		return []string{fmt.Sprintf("table '%s' defines both HydrateDependencies and HydrateConfig", t.Name)}
	}

	var validationErrors []string

	if cyclicDependencyError := t.detectCyclicHydrateDependencies(); cyclicDependencyError != "" {
		validationErrors = append(validationErrors, cyclicDependencyError)
	}
	return validationErrors
}

// ensure that no hydrate config is declared for a hydrate func which has global hydrate config
func (t *Table) validateHydrateConfig() {
	var validationErrors []string
	for funcName := range t.hydrateConfigMap {
		if _, globalConfigExists := t.Plugin.hydrateConfigMap[funcName]; globalConfigExists {
			validationErrors = append(validationErrors, fmt.Sprintf("table '%s' declares HydrateConfig for '%s' which also has global HydrateConfig declared by the plugin", t.Name, funcName))
		}
	}
}

func (t *Table) detectCyclicHydrateDependencies() string {
	var dependencyGraph = topsort.NewGraph()
	dependencyGraph.AddNode("root")

	updateDependencyGraph := func(namedHydrateFunc namedHydrateFunc, hydrateDepends []HydrateFunc) {
		name := namedHydrateFunc.Name
		if !dependencyGraph.ContainsNode(name) {
			dependencyGraph.AddNode(name)
		}
		dependencyGraph.AddEdge("root", name)
		for _, dep := range hydrateDepends {
			depName := newNamedHydrateFunc(dep).Name
			if !dependencyGraph.ContainsNode(depName) {
				dependencyGraph.AddNode(depName)
			}
			dependencyGraph.AddEdge(name, depName)
		}
	}

	for _, hydrateConfig := range t.hydrateConfigMap {
		updateDependencyGraph(hydrateConfig.namedHydrate, hydrateConfig.Depends)
	}

	if _, err := dependencyGraph.TopSort("root"); err != nil {
		return strings.Replace(err.Error(), "Cycle error", "Hydration dependencies contains cycle: ", 1)
	}

	return ""
}

// validate key columns
// also set key column require and operators to default value if not specified
func (t *Table) validateKeyColumns() []string {
	// get key columns should only have equals operators
	var getValidationErrors []string
	var listValidationErrors []string
	if t.Get != nil && len(t.Get.KeyColumns) > 0 {
		getValidationErrors = t.Get.KeyColumns.Validate()
		if !t.Get.KeyColumns.AllEquals() {
			getValidationErrors = append(getValidationErrors, fmt.Sprintf("table '%s' Get key columns must only use '=' operators", t.Name))
		}
		// ensure all key columns actually exist
		getValidationErrors = append(getValidationErrors, t.validateColumnsExist(t.Get.KeyColumns)...)
		if len(getValidationErrors) > 0 {
			getValidationErrors = append([]string{fmt.Sprintf("table '%s' has an invalid Get config:", t.Name)}, helpers.TabifyStringSlice(getValidationErrors, "    - ")...)
		}
	}

	if t.List != nil && len(t.List.KeyColumns) > 0 {
		listValidationErrors = t.List.KeyColumns.Validate()
		if len(listValidationErrors) > 0 {
			listValidationErrors = append([]string{fmt.Sprintf("table '%s' has an invalid List config:", t.Name)}, helpers.TabifyStringSlice(listValidationErrors, "    - ")...)
		}
		// ensure all key columns actually exist
		listValidationErrors = append(listValidationErrors, t.validateColumnsExist(t.List.KeyColumns)...)
	}

	return append(getValidationErrors, listValidationErrors...)
}

func (t *Table) validateColumnsExist(keyColumns KeyColumnSlice) []string {
	var res []string
	for _, c := range keyColumns {
		if t.getColumn(c.Name) == nil {
			res = append(res, fmt.Sprintf("key column '%s' does not exist in table '%s'", c.Name, t.Name))
		}
	}
	return res
}

func (t *Table) validateReservedColumns() (validationWarnings, validationErrors []string) {
	for columnName := range t.columnNameMap {
		if IsReservedColumnName(columnName) {
			// if this is a static plugin, it is an error to use reserved columns. For dynamic plugins itr is a warning
			if t.Plugin.SchemaMode == SchemaModeDynamic {
				validationWarnings = append(validationWarnings, fmt.Sprintf("table '%s': column name '%s' is a reserved name and will be ignored", t.Name, columnName))
			} else {
				validationErrors = append(validationErrors, fmt.Sprintf("table '%s': column name %s is a reserved name", t.Name, columnName))
			}
		}
	}
	return
}

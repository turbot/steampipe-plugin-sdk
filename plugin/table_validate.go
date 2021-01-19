package plugin

import (
	"fmt"
	"log"
	"strings"

	"github.com/stevenle/topsort"
	"github.com/turbotio/go-kit/helpers"
)

func (t *Table) validate(name string, requiredColumns []*Column) []string {
	log.Printf("[TRACE] validate table %s, required columns %v", t.Name, requiredColumns)
	var validationErrors []string
	// does table have a name set?
	if t.Name == "" {
		validationErrors = append(validationErrors, fmt.Sprintf("table with key '%s' in plugin table map does not have a name property set", name))
	}
	// verify all required columns exist
	validationErrors = t.validateRequiredColumns(requiredColumns)

	// validated list config
	validationErrors = append(validationErrors, t.validateListAndGetConfig()...)

	// verify hydrate dependencies are valid
	// the map entries are strings - ensure they correpond to actual functions
	validationErrors = append(validationErrors, t.validateHydrateDependencies()...)

	return validationErrors
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

func (t *Table) validateListAndGetConfig() []string {
	var validationErrors []string
	// either get or list must be defined
	if t.List == nil && t.Get == nil {
		validationErrors = append(validationErrors, fmt.Sprintf("table '%s' does not have either GetConfig or ListConfig - one of these must be provided", t.Name))
	}

	if t.Get != nil {
		if t.Get.Hydrate == nil {
			validationErrors = append(validationErrors, fmt.Sprintf("table '%s' GetConfig does not specify a hydrate function", t.Name))
		}
		if t.Get.KeyColumns == nil {
			validationErrors = append(validationErrors, fmt.Sprintf("table '%s' GetConfig does not specify a key", t.Name))
		}
	}
	if t.List != nil {
		if t.List.Hydrate == nil {
			validationErrors = append(validationErrors, fmt.Sprintf("table '%s' ListConfig does not specify a hydrate function", t.Name))
		}
	}
	return validationErrors
}

func (t *Table) validateHydrateDependencies() []string {
	var validationErrors []string
	if t.HydrateDependencies != nil {
		if t.List != nil {
			deps := t.getHydrateDependencies(helpers.GetFunctionName(t.List.Hydrate))
			if len(deps) > 0 {
				validationErrors = append(validationErrors, fmt.Sprintf("table '%s' List hydrate function '%s' has %d dependencies - List hydrate functions cannot have dependencies", t.Name, helpers.GetFunctionName(t.List.Hydrate), len(deps)))
			}
		}
		if t.Get != nil {
			deps := t.getHydrateDependencies(helpers.GetFunctionName(t.Get.Hydrate))
			if len(deps) > 0 {
				validationErrors = append(validationErrors, fmt.Sprintf("table '%s' Get hydrate function '%s' has %d dependencies - Get hydrate functions cannot have dependencies", t.Name, helpers.GetFunctionName(t.Get.Hydrate), len(deps)))
			}
		}
	}
	if cyclicDependencyError := t.detectCyclicHydrateDependencies(); cyclicDependencyError != "" {
		validationErrors = append(validationErrors, cyclicDependencyError)
	}
	return validationErrors
}

func (t *Table) detectCyclicHydrateDependencies() string {
	var dependencies = topsort.NewGraph()
	dependencies.AddNode("root")
	for _, hydrateDeps := range t.HydrateDependencies {
		name := helpers.GetFunctionName(hydrateDeps.Func)
		if !dependencies.ContainsNode(name) {
			dependencies.AddNode(name)
		}
		dependencies.AddEdge("root", name)
		for _, dep := range hydrateDeps.Depends {
			depName := helpers.GetFunctionName(dep)
			if !dependencies.ContainsNode(depName) {
				dependencies.AddNode(depName)
			}
			dependencies.AddEdge(name, depName)
		}
	}

	if _, err := dependencies.TopSort("root"); err != nil {
		return strings.Replace(err.Error(), "Cycle error", "Hydration dependencies contains cycle: ", 1)
	}

	return ""
}

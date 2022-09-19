package plugin

import (
	"fmt"
	"log"
	"strings"

	"github.com/turbot/go-kit/helpers"
)

func (p *Plugin) validate() string {
	// TODO KAI for dynamic schema we must validate all table maps in ConnectionMap

	log.Printf("[TRACE] validate plugin %s, required columns %v", p.Name, p.RequiredColumns)
	var validationErrors []string
	for tableName, table := range p.TableMap {
		validationErrors = append(validationErrors, table.validate(tableName, p.RequiredColumns)...)
	}
	if p.ConnectionConfigSchema != nil {
		validationErrors = append(validationErrors, p.ConnectionConfigSchema.Validate()...)
	}

	// validate the schema mode
	if !helpers.StringSliceContains(validSchemaModes, p.SchemaMode) {
		validationErrors = append(validationErrors, fmt.Sprintf("schema mode must be either %s or %s (if not specified it defaults to %s)", SchemaModeStatic, SchemaModeDynamic, SchemaModeStatic))
	}

	log.Printf("[TRACE] validate DefaultRetryConfig")
	validationErrors = append(validationErrors, p.DefaultRetryConfig.validate(nil)...)

	log.Printf("[TRACE] validate DefaultIgnoreConfig")
	validationErrors = append(validationErrors, p.DefaultIgnoreConfig.validate(nil)...)

	log.Printf("[TRACE] validate table names")
	validationErrors = append(validationErrors, p.validateTableNames()...)

	log.Printf("[TRACE] plugin has %d validation errors", len(validationErrors))
	return strings.Join(validationErrors, "\n")
}

// validate that table names are consistent with their key in the table map
func (p *Plugin) validateTableNames() []string {
	var validationErrors []string
	for tableName, table := range p.TableMap {
		if table.Name != tableName {
			validationErrors = append(validationErrors, fmt.Sprintf("table '%s' has inconsistent Name property: '%s'", tableName, table.Name))
		}
	}
	return validationErrors
}

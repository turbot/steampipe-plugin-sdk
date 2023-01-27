package plugin

import (
	"fmt"
	"log"
	"strings"
)

func (p *Plugin) validate(tableMap map[string]*Table) string {
	log.Printf("[TRACE] validate plugin %s, required columns %v", p.Name, p.RequiredColumns)
	var validationErrors []string
	for tableName, table := range tableMap {
		validationErrors = append(validationErrors, table.validate(tableName, p.RequiredColumns)...)
	}
	if p.ConnectionConfigSchema != nil {
		validationErrors = append(validationErrors, p.ConnectionConfigSchema.Validate()...)
	}

	// validate the schema mode
	if err := ValidateSchemaMode(p.SchemaMode); err != nil {
		validationErrors = append(validationErrors, err.Error())
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

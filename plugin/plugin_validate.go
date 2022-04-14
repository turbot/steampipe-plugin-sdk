package plugin

import (
	"fmt"
	"log"
	"strings"

	"github.com/turbot/go-kit/helpers"
)

func (p *Plugin) Validate() string {
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
	validationErrors = append(validationErrors, p.DefaultRetryConfig.Validate(nil)...)

	log.Printf("[TRACE] validate DefaultIgnoreConfig")
	validationErrors = append(validationErrors, p.DefaultIgnoreConfig.Validate(nil)...)

	log.Printf("[TRACE] plugin has %d validation errors", len(validationErrors))
	return strings.Join(validationErrors, "\n")
}

package plugin

import (
	"fmt"
	"github.com/gertd/go-pluralize"
	"github.com/turbot/go-kit/helpers"
	"log"
)

func (p *Plugin) validate(tableMap map[string]*Table) (validationWarnings, validationErrors []string) {
	log.Printf("[TRACE] validate plugin %s, required columns %v", p.Name, p.RequiredColumns)
	for tableName, table := range tableMap {
		w, e := table.validate(tableName, p.RequiredColumns)
		validationWarnings = append(validationWarnings, w...)
		validationErrors = append(validationErrors, e...)
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

	log.Printf("[TRACE] validate rate limiters")
	validationErrors = append(validationErrors, p.validateRateLimiters()...)

	log.Printf("[INFO] plugin validation result: %d %s %d %s",
		len(validationWarnings),
		pluralize.NewClient().Pluralize("warning", len(validationWarnings), false),
		len(validationErrors),
		pluralize.NewClient().Pluralize("error", len(validationErrors), false))

	// dedupe the errors and warnins
	validationWarnings = helpers.SortedMapKeys(helpers.SliceToLookup(validationWarnings))
	validationErrors = helpers.SortedMapKeys(helpers.SliceToLookup(validationErrors))

	return validationWarnings, validationErrors
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

// validate all rate limiters
func (p *Plugin) validateRateLimiters() []string {
	log.Printf("[INFO] validateRateLimiters")
	var validationErrors []string
	// intialise and validate each limiter
	// NOTE: we do not need to validate any limiters defined in config and set via SetRateLimiters GRPC call
	// as these are validated when added
	// So we can use RateLimiters property, not resolvedRateLimiterDefs
	for _, l := range p.RateLimiters {
		if err := l.Initialise(); err != nil {
			validationErrors = append(validationErrors, err.Error())
			continue
		}
		// initialised ok, now validate
		validationErrors = append(validationErrors, l.Validate()...)
	}

	return validationErrors
}

package plugin

import (
	"log"
	"strings"
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
	if p.TableMap != nil && p.TableMapFunc != nil {
		validationErrors = append(validationErrors, "plugin defines both TableMap and TableMapFunc")
	}

	return strings.Join(validationErrors, "\n")
}

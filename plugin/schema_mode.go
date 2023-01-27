package plugin

import "fmt"

func ValidateSchemaMode(m string) error {
	if !validSchemaModes[m] {
		return fmt.Errorf("schema mode must be either %s or %s (if not specified it defaults to %s)", SchemaModeStatic, SchemaModeDynamic, SchemaModeStatic)
	}
	return nil
}

const (
	SchemaModeStatic  = "static"
	SchemaModeDynamic = "dynamic"
)

var validSchemaModes = map[string]bool{
	SchemaModeStatic: true, SchemaModeDynamic: true}

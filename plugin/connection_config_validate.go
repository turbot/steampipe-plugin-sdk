package plugin

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/turbot/go-kit/helpers"

	"github.com/turbot/steampipe-plugin-sdk/plugin/schema"
)

// Validate :: validate the connection config
func (c *ConnectionConfigSchema) Validate() []string {
	var validationErrors []string
	if c.NewInstance == nil {
		return []string{"connection config schema does not specify a NewInstance function"}
	}
	instance := c.NewInstance()
	// verify Config.NewInstance() returns a pointer
	kind := reflect.TypeOf(c.NewInstance()).Kind()
	if kind != reflect.Ptr {
		validationErrors = append(validationErrors, fmt.Sprintf("NewInstance function must return a pointer to a struct instance, got %v", kind))
	}

	for name, attr := range c.Schema {
		if attr.Type != schema.TypeList && attr.Elem != nil {
			validationErrors = append(validationErrors, fmt.Sprintf("attribute %s has 'Elem' set but is Type is not TypeList", name))
		}

		// find a property in the struct which is tagged with this field
		validationErrors = append(validationErrors, c.validateConfigStruct(name, attr, instance)...)
	}
	return validationErrors
}

// check all fields in the schema have corresponding tagged struct properties
// check properties for optional fields are nullable
func (c *ConnectionConfigSchema) validateConfigStruct(property string, attr *schema.Attribute, instance interface{}) []string {
	var validationErrors []string
	// find a property in the struct which is annotated with this field
	instance = helpers.DereferencePointer(instance)
	t := reflect.TypeOf(instance)

	var field *reflect.StructField

	// Iterate over all available fields and read the tag value
	for i := 0; i < t.NumField(); i++ {
		// Get the field, returns https://golang.org/pkg/reflect/#StructField
		f := t.Field(i)
		// Get the 'cty' tag value
		tag := f.Tag.Get("cty")
		if tag == "" {
			continue
		}
		// get the first segment of the tag
		tagField := strings.Split(tag, ",")[0]
		if tagField == property {
			field = &f
		}
	}
	if field == nil {
		validationErrors = append(validationErrors, fmt.Sprintf("No structure field with tagged for property %s", property))
	} else if !attr.Required && !nullable(field.Type.Kind()) {
		// if field is optional, the struct property must be nullable
		validationErrors = append(validationErrors, fmt.Sprintf("config structure '%s' is invalid:  optional field '%s' is mapped to %s property '%s' - optional fields must map to a type with a null zero value (struct, array, map or pointer)", t.Name(), property, field.Type.Name(), field.Name))
	}

	return validationErrors
}

func nullable(kind reflect.Kind) bool {
	return kind == reflect.Ptr || kind == reflect.Array || kind == reflect.Interface || kind == reflect.Slice
}

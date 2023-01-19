package plugin

import (
	"fmt"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/schema"

	"reflect"
	"testing"
)

type validateConfigTest struct {
	instance interface{}
	schema   *ConnectionConfigSchema
	expected []string
}
type allProperties struct {
	Regions []string `cty:"regions"`
	Region  string   `cty:"region"`
	Count   int      `cty:"count"`
	Pi      float64  `cty:"pi"`
}
type allPropertiesStringPtr struct {
	Regions []string `cty:"regions"`
	Region  *string  `cty:"region"`
	Count   int      `cty:"count"`
	Pi      float64  `cty:"pi"`
}
type missingTag struct {
	Regions []string
	Region  string  `cty:"region"`
	Count   int     `cty:"count"`
	Pi      float64 `cty:"pi"`
}
type wrongTag struct {
	Regions []string `cty:"regions_foo"`
	Region  string   `cty:"region"`
	Count   int      `cty:"count"`
	Pi      float64  `cty:"pi"`
}
type missingProperty struct {
	Region string  `cty:"region"`
	Count  int     `cty:"count"`
	Pi     float64 `cty:"pi"`
}

var testCasesValidateConfig = map[string]validateConfigTest{
	"pass validation": {
		instance: &allProperties{},
		schema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allProperties{} },
			Schema: map[string]*schema.Attribute{
				"regions": {
					Type:     schema.TypeList,
					Elem:     &schema.Attribute{Type: schema.TypeString},
					Required: true,
				},
				"region": {
					Type:     schema.TypeString,
					Required: true,
				},
				"count": {
					Type:     schema.TypeInt,
					Required: true,
				},
				"pi": {
					Type:     schema.TypeFloat,
					Required: true,
				},
			},
		},
		expected: nil,
	},
	"optional string property": {
		instance: &allProperties{},
		schema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allProperties{} },
			Schema: map[string]*schema.Attribute{
				"regions": {
					Type:     schema.TypeList,
					Elem:     &schema.Attribute{Type: schema.TypeString},
					Required: true,
				},
				"region": {
					Type: schema.TypeString,
				},
				"count": {
					Type:     schema.TypeInt,
					Required: true,
				},
				"pi": {
					Type:     schema.TypeFloat,
					Required: true,
				},
			},
		},
		expected: []string{"config structure 'allProperties' is invalid:  optional field 'region' is mapped to string property 'Region' - optional fields must map to a type with a null zero value (struct, array, map or pointer)"},
	},
	"optional string pointer property": {
		instance: &allProperties{},
		schema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allPropertiesStringPtr{} },
			Schema: map[string]*schema.Attribute{
				"regions": {
					Type:     schema.TypeList,
					Elem:     &schema.Attribute{Type: schema.TypeString},
					Required: true,
				},
				"region": {
					Type: schema.TypeString,
				},
				"count": {
					Type:     schema.TypeInt,
					Required: true,
				},
				"pi": {
					Type:     schema.TypeFloat,
					Required: true,
				},
			},
		},
		expected: nil,
	},
	"missing tag": {
		instance: &allProperties{},
		schema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &missingTag{} },
			Schema: map[string]*schema.Attribute{
				"regions": {
					Type:     schema.TypeList,
					Elem:     &schema.Attribute{Type: schema.TypeString},
					Required: true,
				},
				"region": {
					Type:     schema.TypeString,
					Required: true,
				},
				"count": {
					Type:     schema.TypeInt,
					Required: true,
				},
				"pi": {
					Type:     schema.TypeFloat,
					Required: true,
				},
			},
		},
		expected: []string{"No structure field with tagged for property regions"},
	},
	"wrong tag": {
		instance: &allProperties{},
		schema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &wrongTag{} },
			Schema: map[string]*schema.Attribute{
				"regions": {
					Type:     schema.TypeList,
					Elem:     &schema.Attribute{Type: schema.TypeString},
					Required: true,
				},
				"region": {
					Type:     schema.TypeString,
					Required: true,
				},
				"count": {
					Type:     schema.TypeInt,
					Required: true,
				},
				"pi": {
					Type:     schema.TypeFloat,
					Required: true,
				},
			},
		},
		expected: []string{"No structure field with tagged for property regions"},
	},
	"missing property": {
		instance: &allProperties{},
		schema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &missingProperty{} },
			Schema: map[string]*schema.Attribute{
				"regions": {
					Type:     schema.TypeList,
					Elem:     &schema.Attribute{Type: schema.TypeString},
					Required: true,
				},
				"region": {
					Type:     schema.TypeString,
					Required: true,
				},
				"count": {
					Type:     schema.TypeInt,
					Required: true,
				},
				"pi": {
					Type:     schema.TypeFloat,
					Required: true,
				},
			},
		},
		expected: []string{"No structure field with tagged for property regions"},
	},
}

func TestValidateConnectionConfig(t *testing.T) {
	for name, test := range testCasesValidateConfig {

		validationErrors := test.schema.Validate()

		if !reflect.DeepEqual(validationErrors, test.expected) {
			fmt.Printf("")
			t.Errorf(`Test: '%s' FAILED : expected %v, got %v`, name, test.expected, validationErrors)
		}
	}
}

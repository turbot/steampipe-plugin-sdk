package plugin

import (
	"fmt"

	"github.com/turbot/steampipe-plugin-sdk/plugin/schema"

	"reflect"
	"testing"
)

type getParseConfigTest struct {
	source           string
	connectionConfig *ConnectionConfigSchema
	expected         interface{}
}

type arrayProperty struct {
	Regions []string `cty:"regions"`
}
type stringProperty struct {
	Region string `cty:"region"`
}
type intProperty struct {
	Count int `cty:"count"`
}
type floatProperty struct {
	Pi float64 `cty:"pi"`
}
type allTypes struct {
	Regions []string `cty:"regions"`
	Region  string   `cty:"region"`
	Count   int      `cty:"count"`
	Pi      float64  `cty:"pi"`
}
type allTypesMissingProperty struct {
	Region string  `cty:"region"`
	Count  int     `cty:"count"`
	Pi     float64 `cty:"pi"`
}
type extraPropertyNoAnnotation struct {
	Foo     int
	Regions []string `cty:"regions"`
	Region  string   `cty:"region"`
	Count   int      `cty:"count"`
	Pi      float64  `cty:"pi"`
}
type extraPropertyWithAnnotation struct {
	Foo     int      `cty:"foo"`
	Regions []string `cty:"regions"`
	Region  string   `cty:"region"`
	Count   int      `cty:"count"`
	Pi      float64  `cty:"pi"`
}

var testCasesParseConfig = map[string]getParseConfigTest{
	"array property": {
		source: `
regions = ["us-east-1","us-west-2"]
`,
		connectionConfig: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &arrayProperty{} },
			Schema: map[string]*schema.Attribute{
				"regions": {
					Type:     schema.TypeList,
					Elem:     &schema.Attribute{Type: schema.TypeString},
					Required: true,
				},
			},
		},
		expected: arrayProperty{
			Regions: []string{"us-east-1", "us-west-2"},
		},
	},
	"string property": {
		source: `
region = "us-east-1"
`,
		connectionConfig: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &stringProperty{} },
			Schema: map[string]*schema.Attribute{
				"region": {
					Type:     schema.TypeString,
					Required: true,
				},
			},
		},
		expected: stringProperty{
			Region: "us-east-1",
		},
	},
	"count property": {
		source: `
count = 100
`,
		connectionConfig: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &intProperty{} },
			Schema: map[string]*schema.Attribute{
				"count": {
					Type:     schema.TypeInt,
					Required: true,
				},
			},
		},
		expected: intProperty{
			Count: 100,
		},
	},
	"float property": {
		source: `
pi = 3.14
`,
		connectionConfig: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &floatProperty{} },
			Schema: map[string]*schema.Attribute{
				"pi": {
					Type:     schema.TypeFloat,
					Required: true,
				},
			},
		},
		expected: floatProperty{
			Pi: 3.14,
		},
	},
	"all types": {
		source: `
regions = ["us-east-1","us-west-2"]
region = "us-east-1"
count = 100
pi = 3.14
`,
		connectionConfig: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allTypes{} },
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
		expected: allTypes{
			Regions: []string{"us-east-1", "us-west-2"},
			Region:  "us-east-1",
			Count:   100,
			Pi:      3.14,
		},
	},
	"all types - extra hcl property: EXPECTED ERROR": {
		source: `
regions = ["us-east-1","us-west-2"]
region = "us-east-1"
count = 100
pi = 3.14
foo = "bar"
`,
		connectionConfig: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allTypes{} },
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
		expected: "ERROR",
	},
	"all types - struct missing property: EXPECTED ERROR": {
		source: `
regions = ["us-east-1","us-west-2"]
region = "us-east-1"
count = 100
pi = 3.14
`,
		connectionConfig: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allTypesMissingProperty{} },
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
		expected: "ERROR",
	},
	"all types - struct has extra property no annotation": {
		source: `
regions = ["us-east-1","us-west-2"]
region = "us-east-1"
count = 100
pi = 3.14
`,
		connectionConfig: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &extraPropertyNoAnnotation{} },
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
		expected: extraPropertyNoAnnotation{
			Foo:     0,
			Regions: []string{"us-east-1", "us-west-2"},
			Region:  "us-east-1",
			Count:   100,
			Pi:      3.14,
		},
	},
	"all types - struct has extra property with annotation: EXPECTED ERROR": {
		source: `
regions = ["us-east-1","us-west-2"]
region = "us-east-1"
count = 100
pi = 3.14
`,
		connectionConfig: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &extraPropertyWithAnnotation{} },
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
		expected: "ERROR",
	},
	"all types - missing optional": {
		source: `
region = "us-east-1"
count = 100
pi = 3.14
`,
		connectionConfig: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allTypes{} },
			Schema: map[string]*schema.Attribute{
				"regions": {
					Type:     schema.TypeList,
					Elem:     &schema.Attribute{Type: schema.TypeString},
					Optional: true,
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
		expected: allTypes{
			Regions: nil,
			Region:  "us-east-1",
			Count:   100,
			Pi:      3.14,
		},
	},
	"all types - missing required: EXPECTED ERROR": {
		source: `
region = "us-east-1"
count = 100
pi = 3.14
`,
		connectionConfig: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allTypes{} },
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
		expected: "ERROR",
	},
}

func TestParseConnectionConfig(t *testing.T) {
	for name, test := range testCasesParseConfig {

		config, err := test.connectionConfig.Parse(test.source)

		if err != nil {
			if test.expected != "ERROR" {
				t.Errorf("test %s failed with unexpected error: %v", name, err)
			}
			return
		}

		if !reflect.DeepEqual(config, test.expected) {
			fmt.Printf("")
			t.Errorf(`Test: '%s'' FAILED : expected %v, got %v`, name, test.expected, config)
		}
	}
}

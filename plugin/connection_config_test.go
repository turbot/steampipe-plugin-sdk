package plugin

import (
	"fmt"

	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/schema"

	"reflect"
	"testing"
)

type parseConfigTest struct {
	source                 string
	connectionConfigSchema *ConnectionConfigSchema
	expected               interface{}
	expectedFunc           func(interface{}) bool
}

type arrayProperty struct {
	Regions []string `cty:"regions"`
}
type stringProperty struct {
	Region string `cty:"region"`
}
type stringPtrProperty struct {
	Region *string `cty:"region"`
	Count  int     `cty:"count"`
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

var testCasesParseConfig = map[string]parseConfigTest{
	"array property": {
		source: `
	regions = ["us-east-1","us-west-2"]
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
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
		connectionConfigSchema: &ConnectionConfigSchema{
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
	"string pointer property": {
		source: `
	region = "us-east-1"
	count  = 100
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &stringPtrProperty{} },
			Schema: map[string]*schema.Attribute{
				"region": {
					Type:     schema.TypeString,
					Required: true,
				},
				"count": {
					Type: schema.TypeInt,
				},
			},
		},
		expectedFunc: func(res interface{}) bool {
			return *(res.(stringPtrProperty).Region) == "us-east-1"
		},
	},

	"count property": {
		source: `
		count = 100
		`,
		connectionConfigSchema: &ConnectionConfigSchema{
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
		connectionConfigSchema: &ConnectionConfigSchema{
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
		connectionConfigSchema: &ConnectionConfigSchema{
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
		connectionConfigSchema: &ConnectionConfigSchema{
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
		connectionConfigSchema: &ConnectionConfigSchema{
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
		connectionConfigSchema: &ConnectionConfigSchema{
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
		connectionConfigSchema: &ConnectionConfigSchema{
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
	"all types - missing optional array": {
		source: `
	region = "us-east-1"
	count = 100
	pi = 3.14
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allTypes{} },
			Schema: map[string]*schema.Attribute{
				"regions": {
					Type: schema.TypeList,
					Elem: &schema.Attribute{Type: schema.TypeString},
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
	"all types - missing optional string": {
		source: `
	regions = ["us-east-1"]
	count = 100
	pi = 3.14
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allTypes{} },
			Schema: map[string]*schema.Attribute{
				"regions": {
					Type: schema.TypeList,
					Elem: &schema.Attribute{Type: schema.TypeString},
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
		expected: "ERROR",
	},
	"all types - missing optional string pointer": {
		source: `
	count = 100
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &stringPtrProperty{} },
			Schema: map[string]*schema.Attribute{
				"region": {
					Type: schema.TypeString,
				},
				"count": {
					Type: schema.TypeInt,
				},
			},
		},
		expected: stringPtrProperty{
			Count: 100,
		},
	},
	"all types - missing required: EXPECTED ERROR": {
		source: `
		region = "us-east-1"
		count = 100
		pi = 3.14
		`,
		connectionConfigSchema: &ConnectionConfigSchema{
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

		config, err := test.connectionConfigSchema.Parse(test.source)

		if err != nil {
			if test.expected != "ERROR" {
				t.Errorf("test %s failed with unexpected error: %v", name, err)
			}
			continue
		}
		if test.expectedFunc != nil {
			if !test.expectedFunc(config) {
				t.Errorf(`Test: '%s' FAILED : expect verification func failed`, name)
			}
		} else {
			if !reflect.DeepEqual(config, test.expected) {
				fmt.Printf("")
				t.Errorf(`Test: '%s' FAILED : expected %v, got %v`, name, test.expected, config)
			}
		}

	}
}

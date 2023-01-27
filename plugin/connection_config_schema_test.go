package plugin

import (
	"fmt"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/schema"
	"reflect"
	"testing"
)

type parseConfigTest struct {
	source                 string
	connectionConfigSchema *ConnectionConfigSchema
	expected               interface{}
	expectedFunc           func(interface{}) bool
}

// legacy struct versions using cty tags
type childStructCty struct {
	Name string `cty:"name" cty:"name"`
}
type structPropertyCty struct {
	Name childStructCty `cty:"name"`
}
type structSlicePropertyCty struct {
	Tables []configTableCty `cty:"tables"`
}
type configColumnCty struct {
	Name string `cty:"name"`
	Type string `cty:"type"`
}
type configTableCty struct {
	Name    string            `cty:"name"`
	Columns []configColumnCty `cty:"columns"`
}
type arrayPropertyCty struct {
	Regions []string `cty:"regions"`
}
type stringPropertyCty struct {
	Region string `cty:"region"`
}
type stringPtrPropertyCty struct {
	Region *string `cty:"region"`
	Count  int     `cty:"count"`
}
type intPropertyCty struct {
	Count int `cty:"count"`
}
type floatPropertyCty struct {
	Pi float64 `cty:"pi"`
}
type allTypesCty struct {
	Regions []string `cty:"regions"`
	Region  string   `cty:"region"`
	Count   int      `cty:"count"`
	Pi      float64  `cty:"pi"`
}
type allTypesMissingPropertyCty struct {
	Region string  `cty:"region"`
	Count  int     `cty:"count"`
	Pi     float64 `cty:"pi"`
}
type extraPropertyNoAnnotationCty struct {
	Foo     int
	Regions []string `cty:"regions"`
	Region  string   `cty:"region"`
	Count   int      `cty:"count"`
	Pi      float64  `cty:"pi"`
}
type extraPropertyWithAnnotationCty struct {
	Foo     int      `cty:"foo"`
	Regions []string `cty:"regions"`
	Region  string   `cty:"region"`
	Count   int      `cty:"count"`
	Pi      float64  `cty:"pi"`
}

// hcl struct versions

type childStruct struct {
	Name string `hcl:"name" cty:"name"`
}
type structProperty struct {
	Name childStruct `hcl:"name"`
}
type structSliceProperty struct {
	Tables []configTable `hcl:"tables"`
}
type configColumn struct {
	Name string `hcl:"name" cty:"name"`
	Type string `hcl:"type" cty:"type"`
}
type configTable struct {
	Name    string         `hcl:"name" cty:"name"`
	Columns []configColumn `hcl:"columns" cty:"columns"`
}
type arrayProperty struct {
	Regions []string `hcl:"regions"`
}
type stringProperty struct {
	Region string `hcl:"region"`
}
type stringPtrProperty struct {
	Region *string `hcl:"region"`
	Count  int     `hcl:"count"`
}
type intProperty struct {
	Count int `hcl:"count"`
}
type floatProperty struct {
	Pi float64 `hcl:"pi"`
}
type allTypes struct {
	Regions []string `hcl:"regions,optional"`
	Region  string   `hcl:"region"`
	Count   int      `hcl:"count"`
	Pi      float64  `hcl:"pi"`
}
type allTypesMissingProperty struct {
	Region string  `hcl:"region"`
	Count  int     `hcl:"count"`
	Pi     float64 `hcl:"pi"`
}
type extraPropertyNoAnnotation struct {
	Foo     int
	Regions []string `hcl:"regions"`
	Region  string   `hcl:"region"`
	Count   int      `hcl:"count"`
	Pi      float64  `hcl:"pi"`
}
type extraPropertyWithAnnotation struct {
	Foo     int      `hcl:"foo"`
	Regions []string `hcl:"regions"`
	Region  string   `hcl:"region"`
	Count   int      `hcl:"count"`
	Pi      float64  `hcl:"pi"`
}

var testCasesParseConfig = map[string]parseConfigTest{
	// cty tag implementation
	"array property cty": {
		source: `
	regions = ["us-east-1","us-west-2"]
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &arrayPropertyCty{} },
			Schema: map[string]*schema.Attribute{
				"regions": {
					Type:     schema.TypeList,
					Elem:     &schema.Attribute{Type: schema.TypeString},
					Required: true,
				},
			},
		},
		expected: arrayPropertyCty{
			Regions: []string{"us-east-1", "us-west-2"},
		},
	},
	"string property cty": {
		source: `
	region = "us-east-1"
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &stringPropertyCty{} },
			Schema: map[string]*schema.Attribute{
				"region": {
					Type:     schema.TypeString,
					Required: true,
				},
			},
		},
		expected: stringPropertyCty{
			Region: "us-east-1",
		},
	},
	"string pointer property cty": {
		source: `
	region = "us-east-1"
	count  = 100
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &stringPtrPropertyCty{} },
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
			return *(res.(stringPtrPropertyCty).Region) == "us-east-1"
		},
	},
	"count property cty": {
		source: `
		count = 100
		`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &intPropertyCty{} },
			Schema: map[string]*schema.Attribute{
				"count": {
					Type:     schema.TypeInt,
					Required: true,
				},
			},
		},
		expected: intPropertyCty{
			Count: 100,
		},
	},
	"float property cty": {
		source: `
		pi = 3.14
		`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &floatPropertyCty{} },
			Schema: map[string]*schema.Attribute{
				"pi": {
					Type:     schema.TypeFloat,
					Required: true,
				},
			},
		},
		expected: floatPropertyCty{
			Pi: 3.14,
		},
	},
	"all types cty": {
		source: `
		regions = ["us-east-1","us-west-2"]
		region = "us-east-1"
		count = 100
		pi = 3.14
		`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allTypesCty{} },
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
		expected: allTypesCty{
			Regions: []string{"us-east-1", "us-west-2"},
			Region:  "us-east-1",
			Count:   100,
			Pi:      3.14,
		},
	},
	"all types - extra hcl property: EXPECTED ERROR cty": {
		source: `
		regions = ["us-east-1","us-west-2"]
		region = "us-east-1"
		count = 100
		pi = 3.14
		foo = "bar"
		`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allTypesCty{} },
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
	"all types - struct missing property: EXPECTED ERROR cty": {
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
	"all types - struct has extra property no annotation cty": {
		source: `
		regions = ["us-east-1","us-west-2"]
		region = "us-east-1"
		count = 100
		pi = 3.14
		`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &extraPropertyNoAnnotationCty{} },
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
		expected: extraPropertyNoAnnotationCty{
			Foo:     0,
			Regions: []string{"us-east-1", "us-west-2"},
			Region:  "us-east-1",
			Count:   100,
			Pi:      3.14,
		},
	},
	"all types - struct has extra property with annotation: EXPECTED ERROR cty": {
		source: `
		regions = ["us-east-1","us-west-2"]
		region = "us-east-1"
		count = 100
		pi = 3.14
		`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &extraPropertyWithAnnotationCty{} },
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
	"all types - missing optional array cty": {
		source: `
	region = "us-east-1"
	count = 100
	pi = 3.14
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allTypesCty{} },
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
		expected: allTypesCty{
			Regions: nil,
			Region:  "us-east-1",
			Count:   100,
			Pi:      3.14,
		},
	},
	"all types - missing optional string cty": {
		source: `
	regions = ["us-east-1"]
	count = 100
	pi = 3.14
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allTypesCty{} },
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
	"all types - missing optional string pointer cty": {
		source: `
	count = 100
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &stringPtrPropertyCty{} },
			Schema: map[string]*schema.Attribute{
				"region": {
					Type: schema.TypeString,
				},
				"count": {
					Type: schema.TypeInt,
				},
			},
		},
		expected: stringPtrPropertyCty{
			Count: 100,
		},
	},
	"all types - missing required: EXPECTED ERROR cty": {
		source: `
	
		count = 100
		pi = 3.14
		`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allTypesCty{} },
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
		expected: "ERROR",
	},

	// hcl tag implementation
	"struct property": {
		source: `
	name = { name = "foo"}
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &structProperty{} },
		},
		expected: structProperty{childStruct{"foo"}},
	},
	"struct slice property": {
		source: `
  tables = [
    {
      name    = "test1"
      columns = [
        {
          name = "c1"
          type = "string"
        },
        {
          name = "c2"
          type = "string"
        },
      ]
    }
  ]
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &structSliceProperty{} },
		},
		expected: structSliceProperty{
			Tables: []configTable{{
				Name:    "test1",
				Columns: []configColumn{{"c1", "string"}, {"c2", "string"}},
			}}},
	},
	"array property": {
		source: `
	regions = ["us-east-1","us-west-2"]
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &arrayProperty{} },
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
		},
		expected: "ERROR",
	},
	"all types - missing optional string pointer": {
		source: `
	count = 100
	`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &stringPtrProperty{} },
		},
		expected: stringPtrProperty{
			Count: 100,
		},
	},
	"all types - missing required: EXPECTED ERROR": {
		source: `
	
		count = 100
		pi = 3.14
		`,
		connectionConfigSchema: &ConnectionConfigSchema{
			NewInstance: func() interface{} { return &allTypes{} },
		},
		expected: "ERROR",
	},
}

func TestParseConnectionConfig(t *testing.T) {
	for name, test := range testCasesParseConfig {
		config, err := test.connectionConfigSchema.parse(test.source)
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

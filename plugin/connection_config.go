package plugin

import (
	"fmt"
	"log"

	"github.com/hashicorp/hcl/v2/hcldec"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/schema"
	"github.com/zclconf/go-cty/cty/gocty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

/*
ConnectionConfigSchema is a struct that defines custom arguments in the plugin spc file
that are passed to the plugin as [plugin.Connection.Config].

A plugin that uses custom connection config must set [plugin.Plugin.ConnectionConfigSchema].

Usage:

	p := &plugin.Plugin{
		Name: "steampipe-plugin-hackernews",
		ConnectionConfigSchema: &plugin.ConnectionConfigSchema{
			NewInstance: ConfigInstance,
			Schema:      ConfigSchema,
		},
		...
	}

	var ConfigSchema = map[string]*schema.Attribute{
		"max_items": {
			Type: schema.TypeInt,
		},
	}

	func ConfigInstance() interface{} {
		return &hackernewsConfig{}
	}

Plugin examples:
  - [hackernews]

[hackernews]: https://github.com/turbot/steampipe-plugin-hackernews/blob/d14efdd3f2630f0146e575fe07666eda4e126721/hackernews/plugin.go#L13
*/
type ConnectionConfigSchema struct {
	Schema map[string]*schema.Attribute
	// function which returns an instance of a connection config struct
	NewInstance ConnectionConfigInstanceFunc
}

/*
ConnectionConfigInstanceFunc is a function type which returns 'any'.

It is used to implement [plugin.ConnectionConfigSchema.NewInstance].
*/
type ConnectionConfigInstanceFunc func() interface{}

/*
Connection is a struct which is used to store connection config.

The connection config is parsed and stored as [plugin.Plugin.Connection].

The connection may be retrieved by the plugin by calling: [plugin.QueryData.Connection]

Plugin examples:
  - [hackernews]

[hackernews]: https://github.com/turbot/steampipe-plugin-hackernews/blob/d14efdd3f2630f0146e575fe07666eda4e126721/hackernews/connection_config.go#L23
*/
type Connection struct {
	Name string
	// the connection config
	// NOTE: we always pass and store connection config BY VALUE
	Config interface{}
}

// parse function parses the hcl config string into a connection config struct.
//
// The schema and the  struct to parse into are provided by the plugin
func (c *ConnectionConfigSchema) parse(configString string) (config interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] ConnectionConfigSchema parse caught a panic: %v\n", r)
			err = status.Error(codes.Internal, fmt.Sprintf("ConnectionConfigSchema parse failed with panic %v", r))
		}
	}()

	// ensure a schema is set
	if len(c.Schema) == 0 {
		return nil, fmt.Errorf("cannot parse connection config as no config schema is set in the connection config")
	}
	if c.NewInstance == nil {
		return nil, fmt.Errorf("cannot parse connection config as no NewInstance function is specified in the connection config")
	}
	configStruct := c.NewInstance()
	spec := schema.SchemaToObjectSpec(c.Schema)
	parser := hclparse.NewParser()

	file, diags := parser.ParseHCL([]byte(configString), "/")
	if diags.HasErrors() {
		return nil, DiagsToError("Failed to parse connection config", diags)
	}
	value, diags := hcldec.Decode(file.Body, spec, nil)
	if diags.HasErrors() {
		return nil, DiagsToError("Failed to decode connection config", diags)
	}

	// decode into the provided struct
	if err := gocty.FromCtyValue(value, configStruct); err != nil {
		return nil, fmt.Errorf("Failed to marshal parsed config into config struct: %v", err)
	}

	// return the struct by value
	return helpers.DereferencePointer(configStruct), nil
}

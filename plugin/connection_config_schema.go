package plugin

import (
	"fmt"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hcldec"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/schema"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
	"github.com/zclconf/go-cty/cty/gocty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"runtime/debug"
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

	func ConfigInstance() any {
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
type ConnectionConfigInstanceFunc func() any

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
	Config any
}

// parse function parses the hcl config string into a connection config struct.
//
// The schema and the  struct to parse into are provided by the plugin
func (c *ConnectionConfigSchema) parse(config *proto.ConnectionConfig) (_ any, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] ConnectionConfigSchema parse caught a panic: %v\n", r)
			log.Printf("[WARN] stack: %s", debug.Stack())
			err = status.Error(codes.Internal, fmt.Sprintf("ConnectionConfigSchema parse failed with panic %v", r))
		}
	}()

	if c.Schema == nil {
		return c.parseConfigWithHclTags(config)
	}
	return c.parseConfigWithCtyTags(config)
}

// parse for legacy format config struct using cty tags
func (c *ConnectionConfigSchema) parseConfigWithCtyTags(config *proto.ConnectionConfig) (any, error) {
	configString := []byte(config.Config)
	configStruct := c.NewInstance()
	spec := schema.SchemaToObjectSpec(c.Schema)

	// filename and range may not have been passed (for older versions of CLI)
	filename := ""
	startPos := hcl.Pos{}
	if config.DeclRange != nil {
		filename = config.DeclRange.Filename
		startPos = config.DeclRange.Start.ToHcl()
	}
	file, diags := hclsyntax.ParseConfig(configString, filename, startPos)
	if diags.HasErrors() {
		return nil, DiagsToError("Failed to parse connection config", diags)
	}
	value, diags := hcldec.Decode(file.Body, spec, nil)
	if diags.HasErrors() {
		return nil, DiagsToError(fmt.Sprintf("failed to decode connection config for connection '%s'", config.Connection), diags)
	}

	// decode into the provided struct
	if err := gocty.FromCtyValue(value, configStruct); err != nil {
		return nil, fmt.Errorf("failed to marshal parsed config into config struct for connection '%s': %v", config.Connection, err)
	}
	// return the struct by value
	return helpers.DereferencePointer(configStruct), nil
}

func (c *ConnectionConfigSchema) parseConfigWithHclTags(config *proto.ConnectionConfig) (_ any, err error) {
	configString := []byte(config.Config)
	configStruct := c.NewInstance()
	// filename and range may not have been passed (for older versions of CLI)
	filename := ""
	startPos := hcl.Pos{}

	file, diags := hclsyntax.ParseConfig(configString, filename, startPos)
	if diags.HasErrors() {
		return nil, DiagsToError(fmt.Sprintf("failed to parse connection config for connection '%s'", config.Connection), diags)
	}
	_, body, diags := file.Body.PartialContent(&hcl.BodySchema{})
	if diags.HasErrors() {
		return nil, DiagsToError(fmt.Sprintf("failed to parse connection config for connection '%s'", config.Connection), diags)
	}

	evalCtx := &hcl.EvalContext{
		Variables: make(map[string]cty.Value),
		Functions: make(map[string]function.Function),
	}

	moreDiags := gohcl.DecodeBody(body, evalCtx, configStruct)
	diags = append(diags, moreDiags...)
	if diags.HasErrors() {
		return nil, DiagsToError(fmt.Sprintf("failed to parse connection config for connection '%s'", config.Connection), diags)
	}
	// return the struct by value
	return helpers.DereferencePointer(configStruct), nil
}

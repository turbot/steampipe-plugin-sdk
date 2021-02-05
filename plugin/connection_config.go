package plugin

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/hashicorp/hcl/v2"

	"github.com/turbot/steampipe-plugin-sdk/plugin/schema"

	"github.com/hashicorp/hcl/v2/hcldec"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/turbot/go-kit/helpers"
	"github.com/zclconf/go-cty/cty/gocty"
)

type ConnectionConfigInstanceFunc func() interface{}

type ConnectionConfig struct {
	Schema map[string]*schema.Attribute
	// function which returns an instance of a connection config struct
	NewInstance ConnectionConfigInstanceFunc
	// a map of connection to connection config structs
	// NOTE: we always pass and store connection config BY VALUE
	ConfigMap map[string]interface{}
}

func NewConnectionConfig() *ConnectionConfig {
	return &ConnectionConfig{
		ConfigMap: make(map[string]interface{}),
	}
}

// SetConnectionConfig :: parse the conneection config, and populate the ConfigMap for this connection
// NOTE: we always pass and store connection config BY VALUE
func (c *ConnectionConfig) SetConnectionConfig(connectionName, configString string) error {
	// ask plugin for a struct to deserialise the config into
	config, err := c.Parse(configString)
	if err != nil {
		return err
	}
	c.ConfigMap[connectionName] = config
	return nil
}

// Parse :: parse the hcl string into a connection config struct.
// The schema and the  struct to parse into are provided by the plugin
func (c *ConnectionConfig) Parse(configString string) (interface{}, error) {
	configStruct := c.NewInstance()
	spec := schema.SchemaToObjectSpec(c.Schema)
	parser := hclparse.NewParser()
	file, diags := parser.ParseHCL([]byte(configString), "/")
	if diags.HasErrors() {
		return nil, diagsToError("failed to parse connection config", diags)
	}
	value, diags := hcldec.Decode(file.Body, spec, nil)
	if diags.HasErrors() {
		return nil, diagsToError("failed to parse connection config", diags)
	}

	// decode into the provided struct
	if err := gocty.FromCtyValue(value, configStruct); err != nil {
		return nil, err
	}

	// return the struct by value
	return helpers.DereferencePointer(configStruct), nil
}

// convert hcl diags into an error
func diagsToError(prefix string, diags hcl.Diagnostics) error {
	// convert the first diag into an error
	if !diags.HasErrors() {
		return nil
	}
	for _, diag := range diags {
		if diag.Severity == hcl.DiagError {
			errString := fmt.Sprintf("%s: ", diag.Summary)
			if diag.Detail != "" {
				errString += fmt.Sprintf(": %s", diag.Detail)
			}
			return errors.New(errString)
		}
	}
	return diags.Errs()[0]
}

// Validate :: validate the connection config
func (c *ConnectionConfig) Validate() []string {
	var validationErrors []string
	for name, attr := range c.Schema {
		if attr.Type != schema.TypeList && attr.Elem != nil {
			validationErrors = append(validationErrors, fmt.Sprintf("attribute %s has 'Elem' set but is Type is not TypeList", name))
		}
		// verify ConnectionConfig.NewInstance() returns a pointer
		kind := reflect.TypeOf(c.NewInstance()).Kind()
		if kind != reflect.Ptr {
			validationErrors = append(validationErrors, fmt.Sprintf("NewInstance function must return a pointer to a struct instance, got %v", kind))
		}
		if attr.Required != !attr.Optional {
			validationErrors = append(validationErrors, fmt.Sprintf("connection config schema for attribute '%s' is invalid - either 'Required' or 'Optional' must be true", name))
		}
	}
	return validationErrors
}

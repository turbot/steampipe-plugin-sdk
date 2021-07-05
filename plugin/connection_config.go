package plugin

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hcldec"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/plugin/schema"
	"github.com/zclconf/go-cty/cty/gocty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ConnectionConfigInstanceFunc func() interface{}

type Connection struct {
	Name string
	// the connection config
	// NOTE: we always pass and store connection config BY VALUE
	Config interface{}
}

// ConnectionConfigSchema struct is used to define the connection config schema and store the config for each plugin connection
type ConnectionConfigSchema struct {
	Schema map[string]*schema.Attribute
	// function which returns an instance of a connection config struct
	NewInstance ConnectionConfigInstanceFunc
}

func NewConnectionConfigSchema() *ConnectionConfigSchema {
	return &ConnectionConfigSchema{
		Schema: map[string]*schema.Attribute{},
	}
}

// Parse function parses the hcl string into a connection config struct.
// The schema and the  struct to parse into are provided by the plugin
func (c *ConnectionConfigSchema) Parse(configString string) (config interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] ConnectionConfigSchema Parse caught a panic: %v\n", r)
			err = status.Error(codes.Internal, fmt.Sprintf("ConnectionConfigSchema Parse failed with panic %v", r))
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

// DiagsToError converts hcl diags into an error
func DiagsToError(prefix string, diags hcl.Diagnostics) error {
	// convert the first diag into an error
	if !diags.HasErrors() {
		return nil
	}
	errorStrings := []string{fmt.Sprintf("%s", prefix)}
	// store list of messages (without the range) and use for deduping (we may get the same message for multiple ranges)
	errorMessages := []string{}
	for _, diag := range diags {
		if diag.Severity == hcl.DiagError {
			errorString := fmt.Sprintf("%s", diag.Summary)
			if diag.Detail != "" {
				errorString += fmt.Sprintf(": %s", diag.Detail)
			}

			if !helpers.StringSliceContains(errorMessages, errorString) {
				errorMessages = append(errorMessages, errorString)
				// now add in the subject and add to the output array
				if diag.Subject != nil {
					errorString += fmt.Sprintf("\n(%s)", diag.Subject.String())
				}
				errorStrings = append(errorStrings, errorString)

			}
		}
	}
	if len(errorStrings) > 0 {
		errorString := strings.Join(errorStrings, "\n")
		if len(errorStrings) > 1 {
			errorString += "\n"
		}
		return errors.New(errorString)
	}
	return diags.Errs()[0]
}

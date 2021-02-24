package plugin

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hashicorp/hcl/v2"

	"github.com/turbot/steampipe-plugin-sdk/plugin/schema"

	"github.com/hashicorp/hcl/v2/hcldec"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/turbot/go-kit/helpers"
	"github.com/zclconf/go-cty/cty/gocty"
)

type ConnectionConfigInstanceFunc func() interface{}

type Connection struct {
	Name string
	// the connection config
	// NOTE: we always pass and store connection config BY VALUE
	Config interface{}
}

// ConnectionConfigSchema :: struct used to define the connection config schema and store the config for each plugin connection
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

// Parse :: parse the hcl string into a connection config struct.
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
		return nil, DiagsToError("failed to parse connection config", diags)
	}
	value, diags := hcldec.Decode(file.Body, spec, nil)
	if diags.HasErrors() {
		return nil, DiagsToError("failed to parse connection config", diags)
	}

	// decode into the provided struct
	if err := gocty.FromCtyValue(value, configStruct); err != nil {
		return nil, fmt.Errorf("Failed to marshal parsed config into config struct: %v", err)
	}

	// return the struct by value
	return helpers.DereferencePointer(configStruct), nil
}

// convert hcl diags into an error
func DiagsToError(prefix string, diags hcl.Diagnostics) error {
	// convert the first diag into an error
	if !diags.HasErrors() {
		return nil
	}
	errorsStrings := []string{fmt.Sprintf("%s", prefix)}
	for _, diag := range diags {
		if diag.Severity == hcl.DiagError {
			errorString := fmt.Sprintf("%s", diag.Summary)
			if diag.Detail != "" {
				errorString += fmt.Sprintf(": %s", diag.Detail)
			}
			if diag.Context != nil {
				errorString += fmt.Sprintf(" (%s) ", diag.Context.String())
			}
			if !helpers.StringSliceContains(errorsStrings, errorString) {
				errorsStrings = append(errorsStrings, errorString)
			}
		}
	}
	if len(errorsStrings) > 0 {
		errorString := strings.Join(errorsStrings, "\n")
		if len(errorsStrings) > 1 {
			errorString += "\n"
		}
		return errors.New(errorString)
	}
	return diags.Errs()[0]
}

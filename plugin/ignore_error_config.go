package plugin

import (
	"fmt"
	"log"
	"strings"

	"github.com/turbot/go-kit/helpers"
)

/*
IgnoreConfig defines a set of errors that you want Steampipe to ignore and return an empty row.

It is helpful to define the error codes in this struct instead of handling the error response separately at the plugin level.

It can be defined in the [GetConfig], [ListConfig] struct at the table level and also at the plugin level.

# Usage

At the table level:

		Get: &plugin.GetConfig{
			IgnoreConfig: &plugin.IgnoreConfig{
				ShouldIgnoreErrorFunc: isIgnorableErrorPredicate([]string{"Request_ResourceNotFound", "Invalid object identifier"}),
			},
			...
		},

		List: &plugin.ListConfig{
			IgnoreConfig: &plugin.IgnoreConfig{
				ShouldIgnoreErrorFunc: isIgnorableErrorPredicate([]string{"Request_UnsupportedQuery"}),
			},
			...
		},

At the plugin level:

		DefaultGetConfig: &plugin.GetConfig{
			IgnoreConfig: &plugin.IgnoreConfig{
				ShouldIgnoreErrorFunc: isIgnorableErrorPredicate([]string{"Request_ResourceNotFound"}),
			},
		},

Plugin examples:  
	- [azuread]
	- [aws]

[azuread]: https://github.com/turbot/steampipe-plugin-azuread/blob/f4848195931ca4d97a67e930a493f91f63dfe86d/azuread/table_azuread_application.go#L25-L43
[aws]: https://github.com/turbot/steampipe-plugin-aws/blob/a4c89ed0da07413a42b54dc6a5d625c9bdcec16d/aws/table_aws_ec2_transit_gateway_route_table.go#L23-L25
*/
type IgnoreConfig struct {
	ShouldIgnoreErrorFunc ErrorPredicateWithContext
	// deprecated, used ShouldIgnoreErrorFunc
	ShouldIgnoreError ErrorPredicate
}

func (c *IgnoreConfig) String() interface{} {
	var s strings.Builder
	if c.ShouldIgnoreError != nil {
		s.WriteString(fmt.Sprintf("ShouldIgnoreError: %s\n", helpers.GetFunctionName(c.ShouldIgnoreError)))
	}
	if c.ShouldIgnoreErrorFunc != nil {
		s.WriteString(fmt.Sprintf("ShouldIgnoreErrorFunc: %s\n", helpers.GetFunctionName(c.ShouldIgnoreErrorFunc)))
	}
	return s.String()
}
// Checks if it works

func (c *IgnoreConfig) Validate(table *Table) []string {
	if c.ShouldIgnoreError != nil && c.ShouldIgnoreErrorFunc != nil {
		log.Printf("[TRACE] IgnoreConfig validate failed - both ShouldIgnoreError and ShouldIgnoreErrorFunc are defined")
		return []string{fmt.Sprintf("table '%s' both ShouldIgnoreError and ShouldIgnoreErrorFunc are defined", table.Name)}
	}
	return nil
}

func (c *IgnoreConfig) DefaultTo(other *IgnoreConfig) {
	// if not other provided, nothing to do
	if other == nil {
		return
	}
	// if either ShouldIgnoreError or ShouldIgnoreErrorFunc are set, do not default to other
	if c.ShouldIgnoreError != nil || c.ShouldIgnoreErrorFunc != nil {
		log.Printf("[TRACE] IgnoreConfig DefaultTo: config defines a should ignore function so not defaulting to base")
		return
	}

	// legacy func
	if c.ShouldIgnoreError == nil && other.ShouldIgnoreError != nil {
		log.Printf("[TRACE] IgnoreConfig DefaultTo: using base ShouldIgnoreError: %s", helpers.GetFunctionName(other.ShouldIgnoreError))
		c.ShouldIgnoreError = other.ShouldIgnoreError
	}
	if c.ShouldIgnoreErrorFunc == nil && other.ShouldIgnoreErrorFunc != nil {
		log.Printf("[TRACE] IgnoreConfig DefaultTo: using base ShouldIgnoreErrorFunc: %s", helpers.GetFunctionName(other.ShouldIgnoreErrorFunc))
		c.ShouldIgnoreErrorFunc = other.ShouldIgnoreErrorFunc
	}
}

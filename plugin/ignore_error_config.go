package plugin

import (
	"fmt"
	"log"
	"strings"

	"github.com/turbot/go-kit/helpers"
)

/*
IgnoreConfig defines errors to ignore.
When that happens, an empty row is returned.

If a [HydrateFunc] has specific errors that should not block query execution, set [plugin.GetConfig.IgnoreConfig], [plugin.ListConfig.IgnoreConfig] or [plugin.HydrateConfig.IgnoreConfig].

For errors common to many HydrateFuncs, you can define a default IgnoreConfig by setting [plugin.DefaultGetConfig].

Ignore errors from a HydrateFunc that has a GetConfig:

	Get: &plugin.GetConfig{
		IgnoreConfig: &plugin.IgnoreConfig{
			ShouldIgnoreErrorFunc: isIgnorableErrorPredicate([]string{"Request_ResourceNotFound", "Invalid object identifier"}),
		},
		...
	},

Ignore errors from a HydrateFunc that has a ListConfig:

	List: &plugin.ListConfig{
		IgnoreConfig: &plugin.IgnoreConfig{
			ShouldIgnoreErrorFunc: isIgnorableErrorPredicate([]string{"Request_UnsupportedQuery"}),
		},
		...
	},

Ignore errors from a HydrateFunc that has a HydrateConfig:

	HydrateConfig: []plugin.HydrateConfig{
		IgnoreConfig: &plugin.IgnoreConfig{
			ShouldIgnoreErrorFunc: isIgnorableErrorPredicate([]string{"Request_UnsupportedQuery"}),
		},
		...
	},

Ignore errors that may occur in many HydrateFuncs:

	DefaultIgnoreConfig: &plugin.DefaultIgnoreConfig{
		IgnoreConfig: &plugin.IgnoreConfig{
			ShouldIgnoreErrorFunc: isIgnorableErrorPredicate([]string{"Request_ResourceNotFound"}),
		},
		...
	},

Plugin examples:
  - [azuread]
  - [aws]

[azuread]: https://github.com/turbot/steampipe-plugin-azuread/blob/f4848195931ca4d97a67e930a493f91f63dfe86d/azuread/table_azuread_application.go#L25-L43
[aws]: https://github.com/turbot/steampipe-plugin-aws/blob/a4c89ed0da07413a42b54dc6a5d625c9bdcec16d/aws/table_aws_ec2_transit_gateway_route_table.go#L23-L25
*/
type IgnoreConfig struct {
	ShouldIgnoreErrorFunc ErrorPredicateWithContext
	// Deprecated: used ShouldIgnoreErrorFunc
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

func (c *IgnoreConfig) validate(table *Table) []string {
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

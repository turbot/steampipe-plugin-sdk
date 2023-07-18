package plugin

import (
	"fmt"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
)

// TableRateLimiterConfig contains rate limiter configuration for a table call
type TableRateLimiterConfig struct {
	// the hydrate config can define additional rate limiters which apply to this table
	Definitions *rate_limiter.Definitions

	// scope values used to resolve the rate limiter for this table
	// for example:
	// "service": "s3"
	ScopeValues map[string]string
}

func (c *TableRateLimiterConfig) String() string {
	return fmt.Sprintf("Definitions: %s\nStaticScopeValues: %s", c.Definitions, rate_limiter.FormatStringMap(c.ScopeValues))
}

func (c *TableRateLimiterConfig) validate() []string {
	return c.Definitions.Validate()
}

func (c *TableRateLimiterConfig) initialise(table *Table) {
	if c.ScopeValues == nil {
		c.ScopeValues = make(map[string]string)
	}
	// populate scope values with table name
	c.ScopeValues[rate_limiter.RateLimiterScopeTable] = table.Name

	// initialise our definitions
	if c.Definitions == nil {
		c.Definitions = &rate_limiter.Definitions{}
	}

	c.Definitions.Initialise()

}

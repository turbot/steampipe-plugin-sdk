package plugin

import (
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
	"log"
)

// HydrateRateLimiterConfig contains rate limiter configuration for a hydrate call
// including limtier defintions, tag values for this call, cost and max concurrency
type HydrateRateLimiterConfig struct {
	// the hydrate config can define additional rate limiters which apply to this call
	Definitions *rate_limiter.Definitions

	// how expensive is this hydrate call
	// tags values used to resolve the rate limiter for this hydrate call
	// for example:
	// "service": "s3"
	//
	// when resolving a rate limiter for a hydrate call, a map of key values is automatically populated from:
	// - the connection name
	// - quals (with vales as string)
	// - tag specified in the hydrate config
	//
	// this map is then used to find a rate limiter
	TagValues map[string]string
	// roughly - how many API calls does it hit
	Cost int
	// max concurrency - this applies when the get function is ALSO used as a column hydrate function
	MaxConcurrency int
}

func (c *HydrateRateLimiterConfig) String() string {
	return fmt.Sprintf("Definitions: %s\nTagValues: %s\nCost: %d MaxCooncurrency: %d", c.Definitions, rate_limiter.FormatStringMap(c.TagValues), c.Cost, c.MaxConcurrency)
}

func (c *HydrateRateLimiterConfig) validate() []string {
	return c.Definitions.Validate()
}

func (c *HydrateRateLimiterConfig) initialise(hydrateFunc HydrateFunc) {
	if c.TagValues == nil {
		c.TagValues = make(map[string]string)
	}
	c.TagValues[rate_limiter.RateLimiterKeyHydrate] = helpers.GetFunctionName(hydrateFunc)

	// if cost is not set, initialise to 1
	if c.Cost == 0 {
		log.Printf("[TRACE] HydrateRateLimiterConfig initialise - cost is not set - defaulting to 1")
		c.Cost = 1
	}

	// initialise our definitions
	if c.Definitions == nil {
		c.Definitions = &rate_limiter.Definitions{}
	}
	c.Definitions.Initialise()

}

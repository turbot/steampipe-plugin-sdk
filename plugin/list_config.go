package plugin

import (
	"fmt"
	"github.com/gertd/go-pluralize"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
	"log"
)

/*
[ListConfig] defines how to return all rows in the table:

  - The [HydrateFunc] to use.

  - The [key_columns] that may be used to optimize the fetch.

  - The [error_handling] behaviour.

To define a table's List function:

	func tableHackernewsItem(ctx context.Context) *plugin.Table {
		return &plugin.Table{
			Name:        "hackernews_item",
			Description: "This table includes the most recent items posted to Hacker News.",
			List: &plugin.ListConfig{
				Hydrate: itemList,
			},
			...
		}
	}

Examples:
  - [hackernews]

[hackernews]: https://github.com/turbot/steampipe-plugin-hackernews/blob/bbfbb12751ad43a2ca0ab70901cde6a88e92cf44/hackernews/table_hackernews_item.go#L14
*/
type ListConfig struct {
	// the list function, this should stream the list results back using the QueryData object and return nil
	Hydrate HydrateFunc
	// key or keys which are used to uniquely identify rows - used to optimise the list call
	KeyColumns KeyColumnSlice
	// the parent list function - if we list items with a parent-child relationship, this will list the parent items
	ParentHydrate HydrateFunc
	// a function which will return whenther to ignore a given error
	IgnoreConfig *IgnoreConfig
	// a function which will return whenther to retry the call if an error is returned
	RetryConfig *RetryConfig

	Tags       map[string]string
	ParentTags map[string]string

	// Deprecated: Use IgnoreConfig
	ShouldIgnoreError  ErrorPredicate
	namedHydrate       *namedHydrateFunc
	namedParentHydrate *namedHydrateFunc
}

func (c *ListConfig) initialise(table *Table) {
	log.Printf("[TRACE] ListConfig.initialise table %s", table.Name)

	// create RetryConfig if needed
	if c.RetryConfig == nil {
		c.RetryConfig = &RetryConfig{}
	}

	// create DefaultIgnoreConfig if needed
	if c.IgnoreConfig == nil {
		c.IgnoreConfig = &IgnoreConfig{}
	}

	if c.Tags == nil {
		c.Tags = map[string]string{}
	}

	if c.ParentTags == nil {
		c.ParentTags = map[string]string{}
	}
	// add in function name to tags
	c.Tags[rate_limiter.RateLimiterScopeFunction] = c.namedHydrate.Name
	c.ParentTags[rate_limiter.RateLimiterScopeFunction] = c.namedParentHydrate.Name

	// copy the (deprecated) top level ShouldIgnoreError property into the ignore config
	if c.IgnoreConfig.ShouldIgnoreError == nil {
		c.IgnoreConfig.ShouldIgnoreError = c.ShouldIgnoreError
	}

	// default ignore and retry configs to table defaults
	c.RetryConfig.DefaultTo(table.DefaultRetryConfig)
	c.IgnoreConfig.DefaultTo(table.DefaultIgnoreConfig)

	// populate the named hydrate funcs
	n := newNamedHydrateFunc(c.Hydrate)
	c.namedHydrate = &n
	if c.ParentHydrate != nil {
		p := newNamedHydrateFunc(c.ParentHydrate)
		c.namedParentHydrate = &p
	}

	log.Printf("[TRACE] ListConfig.initialise complete: RetryConfig: %s, IgnoreConfig %s", c.RetryConfig.String(), c.IgnoreConfig.String())
}

func (c *ListConfig) Validate(table *Table) []string {
	var validationErrors []string
	if c.Hydrate == nil {
		validationErrors = append(validationErrors, fmt.Sprintf("table '%s' ListConfig does not specify a hydrate function", table.Name))
	}
	if c.RetryConfig != nil {
		validationErrors = append(validationErrors, c.RetryConfig.validate(table)...)
	}
	if c.IgnoreConfig != nil {
		validationErrors = append(validationErrors, c.IgnoreConfig.validate(table)...)
	}

	// ensure that if there is an explicit hydrate config for the list hydrate, it does not declare dependencies
	listHydrateName := table.List.namedHydrate.Name
	for _, h := range table.HydrateConfig {
		if h.namedHydrate.Name == listHydrateName {
			if len(h.Depends) > 0 {
				validationErrors = append(validationErrors, fmt.Sprintf("table '%s' List hydrate function '%s' defines dependencies in its `HydrateConfig`", table.Name, listHydrateName))
			}
			break
		}
	}
	// ensure there is no hydrate dependency declared for the list hydrate
	for _, h := range table.HydrateDependencies {
		if newNamedHydrateFunc(h.Func).Name == listHydrateName {
			numDeps := len(h.Depends)
			validationErrors = append(validationErrors, fmt.Sprintf("table '%s' List hydrate function '%s' has %d %s - List hydrate functions cannot have dependencies",
				table.Name,
				listHydrateName,
				numDeps,
				pluralize.NewClient().Pluralize("dependency", numDeps, false)))
			break
		}
	}

	return validationErrors
}

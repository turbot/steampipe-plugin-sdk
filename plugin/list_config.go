package plugin
import (
	"fmt"
	"log"

	"github.com/gertd/go-pluralize"
	"github.com/turbot/go-kit/helpers"
)

/*

[ListConfig] defines the function that lists all rows of a table, the KeyColumns that may be used to optimize the fetch, and the error-handling behavior.

[ListConfig]: https://steampipe.io/docs/develop/writing-plugins#list-config

Example from [hackernews]:

[hackernews]: https://github.com/turbot/steampipe-plugin-hackernews/blob/bbfbb12751ad43a2ca0ab70901cde6a88e92cf44/hackernews/table_hackernews_item.go#L14

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

—

*/
type ListConfig struct {
	KeyColumns KeyColumnSlice
	// the list function, this should stream the list results back using the QueryData object and return nil
	Hydrate HydrateFunc
	// the parent list function - if we list items with a parent-child relationship, this will list the parent items
	ParentHydrate HydrateFunc
	// deprecated - use IgnoreConfig
	ShouldIgnoreError ErrorPredicate
	IgnoreConfig      *IgnoreConfig
	RetryConfig       *RetryConfig
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
	// copy the (deprecated) top level ShouldIgnoreError property into the ignore config
	if c.IgnoreConfig.ShouldIgnoreError == nil {
		c.IgnoreConfig.ShouldIgnoreError = c.ShouldIgnoreError
	}

	// default ignore and retry configs to table defaults
	c.RetryConfig.DefaultTo(table.DefaultRetryConfig)
	c.IgnoreConfig.DefaultTo(table.DefaultIgnoreConfig)

	log.Printf("[TRACE] ListConfig.initialise complete: RetryConfig: %s, IgnoreConfig %s", c.RetryConfig.String(), c.IgnoreConfig.String())
}

func (c *ListConfig) Validate(table *Table) []string {
	var validationErrors []string
	if c.Hydrate == nil {
		validationErrors = append(validationErrors, fmt.Sprintf("table '%s' ListConfig does not specify a hydrate function", table.Name))
	}
	if c.RetryConfig != nil {
		validationErrors = append(validationErrors, c.RetryConfig.Validate(table)...)
	}
	if c.IgnoreConfig != nil {
		validationErrors = append(validationErrors, c.IgnoreConfig.Validate(table)...)
	}

	// ensure there is no explicit hydrate config for the list config
	listHydrateName := helpers.GetFunctionName(table.List.Hydrate)
	for _, h := range table.HydrateConfig {
		if helpers.GetFunctionName(h.Func) == listHydrateName {
			validationErrors = append(validationErrors, fmt.Sprintf("table '%s' List hydrate function '%s' also has an explicit hydrate config declared in `HydrateConfig`", table.Name, listHydrateName))
			break
		}
	}
	// ensure there is no hydrate dependency declared for the list hydrate
	for _, h := range table.HydrateDependencies {
		if helpers.GetFunctionName(h.Func) == listHydrateName {
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

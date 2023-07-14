package rate_limiter

import (
	"fmt"
	"github.com/gertd/go-pluralize"
	"golang.org/x/time/rate"
	"sort"
	"strings"
)

/*
TODO multi limiter release all but longest limiter
support specifying limiters in plugin config (somehow)
differentiate between column tags and static tags
*/

type Definition struct {
	Limit     rate.Limit
	BurstSize int
	// the tags which identify this limiter instance
	// one limiter instance will be created for each combination of tags which is encountered
	TagNames []string

	// This limiter only applies to these tag values
	TagFilter map[string]string

	// a string representation of the sorted tag list
	tagsString string
}

func (d *Definition) String() string {
	return fmt.Sprintf("Limit(/s): %v, Burst: %d, Tags: %s", d.Limit, d.BurstSize, strings.Join(d.TagNames, ","))
}

func (d *Definition) Initialise() {
	// convert tags into a string for easy comparison
	sort.Strings(d.TagNames)
	d.tagsString = strings.Join(d.TagNames, ",")
}

// GetRequiredTagValues determines whether we haver a value for all tags specified by the Definition
// and if so returns a map of just the required tag values
// NOTE: if we do not have all required tags, RETURN NIL
func (d *Definition) GetRequiredTagValues(allTagValues map[string]string) map[string]string {
	tagValues := make(map[string]string)
	for _, tag := range d.TagNames {
		value, gotValue := allTagValues[tag]
		if !gotValue {
			return nil
		}
		tagValues[tag] = value
	}
	return tagValues
}

func (d *Definition) validate() []string {
	var validationErrors []string
	if d.Limit == 0 {
		validationErrors = append(validationErrors, "rate limiter defintion must have a non-zero limit")
	}
	if d.BurstSize == 0 {
		validationErrors = append(validationErrors, "rate limiter defintion must have a non-zero burst size")
	}
	return validationErrors
}

type Definitions struct {
	Limiters []*Definition
	// if set, do not look for further rate limiters for this call
	FinalLimiter bool

	limiterMap map[string]*Definition
}

// Merge adds all limiters from other definitions (unless we already have a limiter with same tags)
func (c *Definitions) Merge(other *Definitions) {
	if other == nil {
		return
	}
	for _, l := range other.Limiters {
		if _, gotLimiterWithTags := c.limiterMap[l.tagsString]; !gotLimiterWithTags {
			// if we DO NOT already have a limiter with these tags, add
			c.Limiters = append(c.Limiters, l)
		}
	}
	if other.FinalLimiter {
		c.FinalLimiter = true
	}
}

func (c *Definitions) Initialise() {
	c.limiterMap = make(map[string]*Definition)
	for _, l := range c.Limiters {
		// initialise the defintion to build its tags string
		l.Initialise()

		// add to our map
		c.limiterMap[l.tagsString] = l
	}
}

func (c *Definitions) String() string {
	var strs = make([]string, len(c.Limiters))
	for i, d := range c.Limiters {
		strs[i] = d.String()
	}
	return fmt.Sprintf("%d %s:\n%s \n(FinalLimiter: %v)",
		len(c.Limiters),
		pluralize.NewClient().Pluralize("limiter", len(c.Limiters), false),
		strings.Join(strs, "\n"))
}

func (c *Definitions) Validate() []string {
	var validationErrors []string
	for _, d := range c.Limiters {
		validationErrors = append(validationErrors, d.validate()...)
	}
	return validationErrors
}

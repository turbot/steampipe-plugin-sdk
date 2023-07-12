package rate_limiter

import (
	"fmt"
	"github.com/gertd/go-pluralize"
	"golang.org/x/time/rate"
	"sort"
	"strings"
)

/*
bring back multi limiter
release all but longest limiter


when matching linters, only match if ALL limiter tags are populated

limiter defs are by default additive
limiter def can specify to stop searching down tree for other limiters (i.e. obverride base limiters)
support specifying limiters in plugin config (somehow)

differantiate between column tags and static tags


*/

type Config struct {
	Limiters []*definition
	// if set, do not look for further rate limiters for this call
	FinalLimiter bool

	limiterMap map[string]*definition
}

// Merge adds all limiters from other config (unless we already have a limiter with same tags)
func (c *Config) Merge(other *Config) {
	if other == nil {
		return
	}
	for _, l := range other.Limiters {
		if _, gotLimiterWithTags := c.limiterMap[l.tagsString]; !gotLimiterWithTags {
			// if we DO NOT already have a limiter with these tags, add
			c.Limiters = append(c.Limiters, l)
		}
	}
}

func (c *Config) Initialise() {
	c.limiterMap = make(map[string]*definition)
	for _, l := range c.Limiters {
		// initialise the defintion to build its tags string
		l.Initialise()

		// add to our map
		c.limiterMap[l.tagsString] = l
	}
}

func (c *Config) String() string {
	var strs = make([]string, len(c.Limiters))
	for i, d := range c.Limiters {
		strs[i] = d.String()
	}
	return fmt.Sprintf("%d %s:\n%s \n(FinalLimiter: %v)",
		len(c.Limiters),
		pluralize.NewClient().Pluralize("limiter", len(c.Limiters), false),
		strings.Join(strs, "\n"))
}

type definition struct {
	Limit     rate.Limit
	BurstSize int
	// the tags which identify this limiter instance
	// one limiter instance will be created for each combination of tags which is encountered
	TagNames []string
	// a string representation of the sorted tag list
	tagsString string
}

func (d *definition) String() string {
	return fmt.Sprintf("Limit(/s): %v, Burst: %d, Tags: %s", d.Limit, d.BurstSize, strings.Join(d.TagNames, ","))
}

func (d *definition) Initialise() {
	// convert tags into a string for easy comparison
	sort.Strings(d.TagNames)
	d.tagsString = strings.Join(d.TagNames, ",")
}

// GetRequiredTagValues determines whether we haver a value for all tags specified by the definition
// and if so returns a map of just the required tag values
// NOTE: if we do not have all required tags, RETURN NIL
func (d *definition) GetRequiredTagValues(allTagValues map[string]string) map[string]string {
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

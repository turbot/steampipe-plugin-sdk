package rate_limiter

import (
	"fmt"
	"github.com/gertd/go-pluralize"
	"strings"
)

type Definitions struct {
	Limiters []*Definition
	// if set, do not look for further rate limiters for this call
	FinalLimiter bool

	limiterMap map[string]*Definition
}

func (c *Definitions) Initialise() {
	c.limiterMap = make(map[string]*Definition)
	for _, l := range c.Limiters {
		// add to our map (keyeD by the string representation of the scope
		c.limiterMap[l.Scopes.String()] = l
	}
}

// Merge adds all limiters from other definitions
// (unless we already have a limiter with same scopes)
func (c *Definitions) Merge(other *Definitions) {
	if c.FinalLimiter {
		// should NEVER happen as calling code will do this check
		panic("attempting to merge lower precedence Definitions when FinalLimiter=true")
	}
	if other == nil {
		return
	}

	for _, l := range other.Limiters {
		// if we DO NOT already have a limiter with these tags, add
		if _, gotLimiterWithScope := c.limiterMap[l.Scopes.String()]; !gotLimiterWithScope {
			c.add(l)
		}
	}

	// if the other limiter has FinalLimiter set,
	// we will not merge any lower precedence definitions
	if other.FinalLimiter {
		c.FinalLimiter = true
	}
}

func (c *Definitions) add(l *Definition) {
	// add to list
	c.Limiters = append(c.Limiters, l)
	// add to map
	c.limiterMap[l.Scopes.String()] = l
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

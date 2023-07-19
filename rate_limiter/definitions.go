package rate_limiter

import (
	"fmt"
	"github.com/gertd/go-pluralize"
	"strings"
)

// TODO is this needed
type Definitions []*Definition

func (c Definitions) String() string {
	var strs = make([]string, len(c))
	for i, d := range c {
		strs[i] = d.String()
	}
	return fmt.Sprintf("%d %s:\n%s",
		len(c),
		pluralize.NewClient().Pluralize("limiter", len(c), false),
		strings.Join(strs, "\n"))
}

func (c Definitions) Validate() []string {
	var validationErrors []string
	for _, d := range c {
		validationErrors = append(validationErrors, d.validate()...)
	}
	return validationErrors
}

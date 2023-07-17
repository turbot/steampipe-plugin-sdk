package rate_limiter

import (
	"fmt"
	"golang.org/x/time/rate"
)

/*
TODO multi limiter release all but longest limiter
support specifying limiters in plugin config (somehow)
differentiate between column tags and static tags
*/

type Definition struct {
	// the actual limiter config
	Limit     rate.Limit
	BurstSize int

	// the scopes which identify this limiter instance
	// one limiter instance will be created for each combination of scopes which is encountered
	Scopes Scopes

	// this limiter only applies to these these scope values
	Filters []ScopeFilter
}

func (d *Definition) String() string {
	return fmt.Sprintf("Limit(/s): %v, Burst: %d, Scopes: %s", d.Limit, d.BurstSize, d.Scopes)
}

func (d *Definition) validate() []string {
	var validationErrors []string
	if d.Limit == 0 {
		validationErrors = append(validationErrors, "rate limiter definition must have a non-zero limit")
	}
	if d.BurstSize == 0 {
		validationErrors = append(validationErrors, "rate limiter definition must have a non-zero burst size")
	}
	return validationErrors
}

// SatisfiesFilters returns whethe rthe given values satisfy ANY of our filters
func (d *Definition) SatisfiesFilters(scopeValues *ScopeValues) bool {
	// do we satisfy any of the filters
	for _, f := range d.Filters {
		if f.Satisfied(scopeValues) {
			return true
		}
	}
	return false
}

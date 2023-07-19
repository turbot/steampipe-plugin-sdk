package rate_limiter

import (
	"fmt"
	"golang.org/x/time/rate"
)

type Definition struct {
	// the limiter name
	Name string
	// the actual limiter config
	Limit     rate.Limit
	BurstSize int

	// the scopes which identify this limiter instance
	// one limiter instance will be created for each combination of scopes which is encountered
	Scopes []string

	// filter used to target the limiter
	Filter string
}

func (d *Definition) String() string {
	return fmt.Sprintf("Limit(/s): %v, Burst: %d, Scopes: %s, Filter: %s", d.Limit, d.BurstSize, d.Scopes, d.Filter)
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
func (d *Definition) SatisfiesFilters(scopeValues map[string]string) bool {
	//// do we satisfy any of the filters
	//for _, f := range d.Filter {
	//	if f.satisfied(scopeValues) {
	//		return true
	//	}
	//}
	return false
}

package rate_limiter

import (
	"fmt"
	"golang.org/x/time/rate"
	"log"
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
	Filter       string
	parsedFilter *scopeFilter
}

func (d *Definition) Initialise() error {
	log.Printf("[INFO] initialise rate limiter Definition")
	if d.Filter != "" {
		scopeFilter, err := newScopeFilter(d.Filter)
		if err != nil {
			log.Printf("[WARN] failed to parse scope filter: %s", err.Error())
			return err
		}
		log.Printf("[INFO] parsed scope filter %s", d.Filter)
		d.parsedFilter = scopeFilter
	}
	return nil
}

func (d *Definition) String() string {
	return fmt.Sprintf("Limit(/s): %v, Burst: %d, Scopes: %s, Filter: %s", d.Limit, d.BurstSize, d.Scopes, d.Filter)
}

func (d *Definition) Validate() []string {
	var validationErrors []string
	if d.Name == "" {
		validationErrors = append(validationErrors, "rate limiter definition must specify a name")
	}
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
	if d.parsedFilter == nil {
		return true
	}

	return d.parsedFilter.satisfied(scopeValues)
}

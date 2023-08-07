package rate_limiter

import (
	"fmt"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"golang.org/x/time/rate"
	"log"
)

type Definition struct {
	// the limiter name
	Name string
	// the actual limiter config
	FillRate   rate.Limit
	BucketSize int64

	MaxConcurrency int64
	// the scope properties which identify this limiter instance
	// one limiter instance will be created for each combination of these properties which is encountered
	Scope []string

	// filter used to target the limiter
	Where        string
	parsedFilter *scopeFilter
}

// DefinitionFromProto converts the proto format RateLimiterDefinition into a Defintion
func DefinitionFromProto(p *proto.RateLimiterDefinition) (*Definition, error) {
	var res = &Definition{
		Name:           p.Name,
		FillRate:       rate.Limit(p.FillRate),
		BucketSize:     p.BucketSize,
		MaxConcurrency: p.MaxConcurrency,
		Scope:          p.Scope,
		Where:          p.Where,
	}
	if err := res.Initialise(); err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Definition) Initialise() error {
	log.Printf("[INFO] initialise rate limiter Definition")
	if d.Where != "" {
		scopeFilter, err := newScopeFilter(d.Where)
		if err != nil {
			log.Printf("[WARN] failed to parse scope filter: %s", err.Error())
			return err
		}
		log.Printf("[INFO] parsed scope filter %s", d.Where)
		d.parsedFilter = scopeFilter
	}
	return nil
}

func (d *Definition) String() string {
	return fmt.Sprintf("Limit(/s): %v, Burst: %d, Scopes: %s, Filter: %s", d.FillRate, d.BucketSize, d.Scope, d.Where)
}

func (d *Definition) Validate() []string {
	var validationErrors []string
	if d.Name == "" {
		validationErrors = append(validationErrors, "rate limiter definition must specify a name")
	}
	if d.FillRate == 0 {
		validationErrors = append(validationErrors, "rate limiter definition must have a non-zero limit")
	}
	if d.BucketSize == 0 {
		validationErrors = append(validationErrors, "rate limiter definition must have a non-zero burst size")
	}

	return validationErrors
}

// SatisfiesFilters returns whether the given values satisfy ANY of our filters
func (d *Definition) SatisfiesFilters(scopeValues map[string]string) bool {
	if d.parsedFilter == nil {
		return true
	}

	return d.parsedFilter.satisfied(scopeValues)
}

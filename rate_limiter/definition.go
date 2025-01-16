package rate_limiter

import (
	"fmt"
	"golang.org/x/time/rate"
	"log"
	"regexp"
	"strings"

	"github.com/turbot/pipe-fittings/filter"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

type Definition struct {
	// the limiter name
	Name string
	// the actual limiter config
	FillRate   rate.Limit
	BucketSize int64
	// the max concurrency supported
	MaxConcurrency int64
	// the scope properties which identify this limiter instance
	// one limiter instance will be created for each combination of these properties which is encountered
	Scope []string

	// filter used to target the limiter
	Where        string
	parsedFilter *filter.SqlFilter
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

func (d *Definition) ToProto() *proto.RateLimiterDefinition {
	return &proto.RateLimiterDefinition{
		Name:           d.Name,
		FillRate:       float32(d.FillRate),
		BucketSize:     d.BucketSize,
		MaxConcurrency: d.MaxConcurrency,
		Scope:          d.Scope,
		Where:          d.Where,
	}
}

func (d *Definition) Initialise() error {
	log.Printf("[INFO] initialise rate limiter Definition")
	if d.Where != "" {
		scopeFilter, err := filter.NewSqlFilter(d.Where)
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
	limiterString := ""
	concurrencyString := ""
	if d.FillRate >= 0 {
		limiterString = fmt.Sprintf("Limit(/s): %v, Burst: %d", d.FillRate, d.BucketSize)
	}
	if d.MaxConcurrency >= 0 {
		concurrencyString = fmt.Sprintf("MaxConcurrency: %d", d.MaxConcurrency)
	}
	return fmt.Sprintf("%s Scopes: %s, Where: %s", strings.Join([]string{limiterString, concurrencyString}, " "), d.Scope, d.Where)
}

func (d *Definition) Validate() []string {
	var validationErrors []string
	if d.Name == "" {
		validationErrors = append(validationErrors, "rate limiter definition must specify a name")
	}
	if !validHCLLabel(d.Name) {
		validationErrors = append(validationErrors, fmt.Sprintf("invalid rate limiter name '%s' - names can contain letters, digits, underscores (_), and hyphens (-), and cannot start with a digit", d.Name))
	}
	if (d.FillRate == 0 || d.BucketSize == 0) && d.MaxConcurrency == 0 {
		validationErrors = append(validationErrors, "rate limiter definition must definer either a rate limit or max concurrency")
	}

	return validationErrors
}

func validHCLLabel(name string) bool {
	// Identifiers can contain letters, digits, underscores (_), and hyphens (-). The first character of an identifier must not be a digit, to avoid ambiguity with literal numbers.
	return regexp.MustCompile(`^[a-zA-Z0-9_-]+$`).MatchString(name) &&
		// must not start with number (no negative lookaheads in go :( )
		!regexp.MustCompile(`^[0-9]+$`).MatchString(name[:1])
}

// SatisfiesFilters returns whether the given values satisfy ANY of our filters
func (d *Definition) SatisfiesFilters(scopeValues map[string]string) bool {
	if d.parsedFilter == nil {
		return true
	}

	return d.parsedFilter.Satisfied(scopeValues)
}

package rate_limiter

import (
	"fmt"
	"github.com/turbot/go-kit/helpers"
)

type ScopeValues struct {
	StaticValues map[string]string
	ColumnValues map[string]string
}

func NewRateLimiterScopeValues() *ScopeValues {
	return &ScopeValues{
		StaticValues: make(map[string]string),
		ColumnValues: make(map[string]string),
	}
}
func (sv *ScopeValues) String() string {
	return fmt.Sprintf("static-values: %s\ncolumn-values: %s",
		helpers.SortedMapKeys(sv.StaticValues),
		helpers.SortedMapKeys(sv.ColumnValues))
}

// Merge adds the given values to our map WITHOUT OVERWRITING existing values
// i.e. we have precedence over otherValues
func (sv *ScopeValues) Merge(otherValues *ScopeValues) {
	if otherValues == nil {
		return
	}
	for k, v := range otherValues.StaticValues {
		// only set tag if not already set - earlier tag values have precedence
		if _, gotValue := sv.StaticValues[k]; !gotValue {
			sv.StaticValues[k] = v
		}
	}
}

func (sv *ScopeValues) count() int {
	return len(sv.StaticValues) + len(sv.ColumnValues)
}

package rate_limiter

import (
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"sort"
	"strings"
)

type Scopes struct {
	StaticScopes []string
	ColumnScopes []string
	// a string representation of the sorted scopes
	scopesString string
}

func (s Scopes) String() string {
	// lazily populate the scopes string
	if s.scopesString == "" {
		s.initializeScopeString()
	}
	return s.scopesString
}

func (s Scopes) GetRequiredValues(values *ScopeValues) (*ScopeValues, bool) {
	requiredValues := NewRateLimiterScopeValues()
	requiredValues.StaticValues = helpers.FilterMap(values.StaticValues, s.StaticScopes)
	requiredValues.ColumnValues = helpers.FilterMap(values.ColumnValues, s.ColumnScopes)

	// do we have all required scope values?
	var gotAllRequiredScopeValues = requiredValues.count() == s.count()
	return requiredValues, gotAllRequiredScopeValues
}

func (s Scopes) initializeScopeString() {
	if s.count() == 0 {
		s.scopesString = "empty"
	}

	var scopesStrs []string
	if len(s.StaticScopes) > 0 {
		// convert tags into a string for easy comparison
		sort.Strings(s.StaticScopes)
		scopesStrs = append(scopesStrs, fmt.Sprintf("static:%s", strings.Join(s.StaticScopes, ",")))
	}
	if len(s.ColumnScopes) > 0 {
		// convert tags into a string for easy comparison
		sort.Strings(s.ColumnScopes)
		scopesStrs = append(scopesStrs, fmt.Sprintf("dynamic:%s", strings.Join(s.ColumnScopes, ",")))
	}
	s.scopesString = strings.Join(scopesStrs, " ")
}

func (s Scopes) count() int {
	return len(s.StaticScopes) + len(s.ColumnScopes)
}

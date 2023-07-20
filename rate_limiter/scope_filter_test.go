package rate_limiter

import (
	"testing"
)

func TestScopeFilterSatisfied(t *testing.T) {
	testCases := []struct {
		filter   string
		values   map[string]string
		expected bool
		err      string
	}{
		{
			filter:   "connection='foo'",
			values:   map[string]string{"connection": "foo"},
			expected: true,
		},
		{
			filter:   "connection='foo'",
			values:   map[string]string{"connection": "bar"},
			expected: false,
		},
		{
			filter:   "connection!='foo'",
			values:   map[string]string{"connection": "foo"},
			expected: false,
		},
		{
			filter:   "connection!='foo'",
			values:   map[string]string{"connection": "bar"},
			expected: true,
		},
		{
			filter:   "connection in ('foo','bar')",
			values:   map[string]string{"connection": "bar"},
			expected: true,
		},
		{
			filter:   "connection in ('foo','bar')",
			values:   map[string]string{"connection": "other"},
			expected: false,
		},
		{
			filter:   "connection not in ('foo','bar')",
			values:   map[string]string{"connection": "other"},
			expected: true,
		},
		{
			filter:   "connection not in ('foo','bar') or connection='hello'",
			values:   map[string]string{"connection": "bar"},
			expected: false,
		},
		{
			filter:   "connection in ('foo','bar') and connection='foo'",
			values:   map[string]string{"connection": "foo"},
			expected: true,
		},
	}
	for _, testCase := range testCases {
		scopeFilter, err := newScopeFilter(testCase.filter)
		if testCase.err != "" {
			if err == nil || err.Error() != testCase.err {
				t.Errorf("parseWhere(%v) err: %v, want %s", testCase.filter, err, testCase.err)
			}
			continue
		}
		if err != nil {
			t.Error(err)
		}

		satisfiesFilter := scopeFilter.satisfied(testCase.values)

		if satisfiesFilter != testCase.expected {
			t.Errorf("scopeFilterSatisfied(%v, %v) want %v, got %v", testCase.filter, testCase.values, testCase.expected, satisfiesFilter)
		}

	}
}

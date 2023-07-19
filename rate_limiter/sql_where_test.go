package rate_limiter

import (
	"testing"
)

func TestWhereSatisfied(t *testing.T) {
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
	}
	for _, testCase := range testCases {
		whereExpr, err := parseWhere(testCase.filter)
		if testCase.err != "" {
			if err == nil || err.Error() != testCase.err {
				t.Errorf("parseWhere(%v) err: %v, want %s", testCase.filter, err, testCase.err)
			}
			continue
		}
		if err != nil {
			t.Error(err)
		}

		satisfiesFilter := whereSatisfied(whereExpr, testCase.values)

		if satisfiesFilter != testCase.expected {
			t.Errorf("whereSatisfied(%v, %v) want %v, got %v", testCase.filter, testCase.values, testCase.expected, satisfiesFilter)
		}

	}
}

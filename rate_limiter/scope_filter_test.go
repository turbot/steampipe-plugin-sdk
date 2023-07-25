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
		//comparisons
		{
			filter:   "connection = 'foo'",
			values:   map[string]string{"connection": "foo"},
			expected: true,
		},
		{
			filter:   "connection = 'foo'",
			values:   map[string]string{"connection": "bar"},
			expected: false,
		},
		{
			filter:   "connection != 'foo'",
			values:   map[string]string{"connection": "foo"},
			expected: false,
		},
		{
			filter:   "connection != 'foo'",
			values:   map[string]string{"connection": "bar"},
			expected: true,
		},
		{
			filter:   "connection != 'foo'",
			values:   map[string]string{"connection": "bar"},
			expected: true,
		},
		{
			filter:   "connection <> 'foo'",
			values:   map[string]string{"connection": "bar"},
			expected: true,
		},
		{
			filter:   "connection <> 'foo'",
			values:   map[string]string{"connection": "bar"},
			expected: true,
		},
		// in
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
		//like
		{
			filter:   "connection like 'fo_'",
			values:   map[string]string{"connection": "foo"},
			expected: true,
		},
		{
			filter:   "connection like 'fo_'",
			values:   map[string]string{"connection": "bar"},
			expected: false,
		},
		{
			filter:   "connection like '_o_'",
			values:   map[string]string{"connection": "foo"},
			expected: true,
		},
		{
			filter:   "connection like '_o_'",
			values:   map[string]string{"connection": "bar"},
			expected: false,
		},
		{
			filter:   "connection like 'f%'",
			values:   map[string]string{"connection": "foo"},
			expected: true,
		},
		{
			filter:   "connection like 'f%'",
			values:   map[string]string{"connection": "bar"},
			expected: false,
		},
		{
			filter:   "connection like '%ob%'",
			values:   map[string]string{"connection": "foobar"},
			expected: true,
		},
		{
			filter:   "connection like '%ob%'",
			values:   map[string]string{"connection": "foo"},
			expected: false,
		},
		{
			filter:   "connection like '_oo%'",
			values:   map[string]string{"connection": "foobar"},
			expected: true,
		},
		{
			filter:   "connection like '_oo%'",
			values:   map[string]string{"connection": "foo"},
			expected: true,
		},
		{
			filter:   "connection like '_oo%'",
			values:   map[string]string{"connection": "bar"},
			expected: false,
		},
		{
			filter:   "connection like 'fo_'",
			values:   map[string]string{"connection": "foo"},
			expected: true,
		},
		{
			filter:   "connection like 'fo_'",
			values:   map[string]string{"connection": "foo"},
			expected: true,
		},
		{
			filter:   "connection like 'FO_'",
			values:   map[string]string{"connection": "FOO"},
			expected: true,
		},
		{
			filter:   "connection like 'FO_'",
			values:   map[string]string{"connection": "foo"},
			expected: false,
		},

		//ilike
		{
			filter:   "connection ilike 'FO_'",
			values:   map[string]string{"connection": "foo"},
			expected: true,
		},
		// not  like
		{
			filter:   "connection not like 'fo_'",
			values:   map[string]string{"connection": "foo"},
			expected: false,
		},
		{
			filter:   "connection not like 'fo_'",
			values:   map[string]string{"connection": "bar"},
			expected: true,
		},
		{
			filter:   "connection not like '_o_'",
			values:   map[string]string{"connection": "foo"},
			expected: false,
		},
		{
			filter:   "connection not like '_o_'",
			values:   map[string]string{"connection": "bar"},
			expected: true,
		},
		{
			filter:   "connection not like 'f%'",
			values:   map[string]string{"connection": "foo"},
			expected: false,
		},
		{
			filter:   "connection not like 'f%'",
			values:   map[string]string{"connection": "bar"},
			expected: true,
		},
		{
			filter:   "connection not like '%ob%'",
			values:   map[string]string{"connection": "foobar"},
			expected: false,
		},
		{
			filter:   "connection not like '%ob%'",
			values:   map[string]string{"connection": "foo"},
			expected: true,
		},
		{
			filter:   "connection not like '_oo%'",
			values:   map[string]string{"connection": "foobar"},
			expected: false,
		},
		{
			filter:   "connection not like '_oo%'",
			values:   map[string]string{"connection": "foo"},
			expected: false,
		},
		{
			filter:   "connection not like '_oo%'",
			values:   map[string]string{"connection": "bar"},
			expected: true,
		},
		{
			filter:   "connection not like 'fo_'",
			values:   map[string]string{"connection": "foo"},
			expected: false,
		},
		{
			filter:   "connection not like 'fo_'",
			values:   map[string]string{"connection": "foo"},
			expected: false,
		},
		{
			filter:   "connection not like 'FO_'",
			values:   map[string]string{"connection": "FOO"},
			expected: false,
		},
		{
			filter:   "connection not like 'FO_'",
			values:   map[string]string{"connection": "foo"},
			expected: true,
		},
		// not ilike
		{
			filter:   "connection not ilike 'FO_'",
			values:   map[string]string{"connection": "foo"},
			expected: false,
		},
		{
			filter:   "connection not ilike 'FO_'",
			values:   map[string]string{"connection": "bar"},
			expected: true,
		},
		//// complex queries
		//{
		//	filter:   "connection not in ('foo','bar') or connection='hello'",
		//	values:   map[string]string{"connection": "bar"},
		//	expected: false,
		//},
		//{
		//	filter:   "connection in ('foo','bar') and connection='foo'",
		//	values:   map[string]string{"connection": "foo"},
		//	expected: true,
		//},
		//{
		//	filter:   "connection in ('foo','bar') and connection='other'",
		//	values:   map[string]string{"connection": "foo"},
		//	expected: false,
		//},
		//{
		//	filter:   "connection in ('a','b') or connection='foo'",
		//	values:   map[string]string{"connection": "foo"},
		//	expected: true,
		//},
		//{
		//	filter:   "connection in ('a','b') or connection='c'",
		//	values:   map[string]string{"connection": "foo"},
		//	expected: false,
		//},

		//// not supported
		//{
		//	// 'is not' not supported
		//	filter: "connection is null",
		//	values: map[string]string{"connection": "foo"},
		//	err:    invalidScopeOperatorError("is").Error(),
		//},
		//{
		//	// 'is' not supported
		//	filter: "connection is not null",
		//	values: map[string]string{"connection": "foo"},
		//	err:    invalidScopeOperatorError("is not").Error(),
		//},
		//{
		//	// '<' is not supported
		//	filter: "connection < 'bar'",
		//	values: map[string]string{"connection": "foo"},
		//	err:    invalidScopeOperatorError("<").Error(),
		//},
		//{
		//	// '<=' is not supported
		//	filter: "connection <= 'bar'",
		//	values: map[string]string{"connection": "foo"},
		//	err:    invalidScopeOperatorError("<=").Error(),
		//},
		//{
		//	// '>' is not supported
		//	filter: "connection > 'bar'",
		//	values: map[string]string{"connection": "foo"},
		//	err:    invalidScopeOperatorError(">").Error(),
		//},
		//{
		//	// '>=' is not supported
		//	filter: "connection >= 'bar'",
		//	values: map[string]string{"connection": "foo"},
		//	err:    invalidScopeOperatorError(">=").Error(),
		//},
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
			t.Fatal(err)
		}

		satisfiesFilter := scopeFilter.satisfied(testCase.values)

		if satisfiesFilter != testCase.expected {
			t.Errorf("scopeFilterSatisfied(%v, %v) want %v, got %v", testCase.filter, testCase.values, testCase.expected, satisfiesFilter)
		}

	}
}

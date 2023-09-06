package rate_limiter

import "testing"

func TestValidHCLLabel(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
	}{
		{"valid", true},
		{"valid1", true},
		{"valid-2", true},
		{"valid_3", true},
		{"valid--4", true},
		{"valid__5", true},
		{"invalid#1", false},
		{"2-invalid2", false},
		{"invalid 3", false},
	}

	for _, testCase := range testCases {
		res := validHCLLabel(testCase.input)
		if res != testCase.expected {
			t.Errorf("failed for '%s', expected %v, got %v", testCase.input, testCase.expected, res)
		}
	}
}

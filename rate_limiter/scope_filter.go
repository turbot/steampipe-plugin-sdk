package rate_limiter

type ScopeFilter struct {
	StaticFilterValues map[string]string
	ColumnFilterValues map[string]string
}

func (f ScopeFilter) satisfied(scopeValues *ScopeValues) bool {
	// all filter values must be satisfied
	// (it is fine if there are scope values with no corresponding filter values)
	for k, filterVal := range f.StaticFilterValues {
		if actualVal := scopeValues.StaticValues[k]; actualVal != filterVal {
			return false
		}
	}

	for k, filterVal := range f.ColumnFilterValues {
		if actualVal := scopeValues.ColumnValues[k]; actualVal != filterVal {
			return false
		}
	}
	return true
}

package query_cache

import "slices"

type CacheMissError struct{}

func (CacheMissError) Error() string { return "cache miss" }

func IsCacheMiss(err error) bool {
	if err == nil {
		return false
	}
	// BigCache returns "Entry not found"
	errorStrings := []string{CacheMissError{}.Error(), "Entry not found"}
	return slices.Contains(errorStrings, err.Error())
}

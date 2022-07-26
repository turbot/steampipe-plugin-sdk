package query_cache

import "github.com/turbot/go-kit/helpers"

type CacheMissError struct{}

func (CacheMissError) Error() string { return "cache miss" }

func IsCacheMiss(err error) bool {
	if err == nil {
		return false
	}
	// BigCache returns "Entry not found"
	errorStrings := []string{CacheMissError{}.Error(), "Entry not found"}
	return helpers.StringSliceContains(errorStrings, err.Error())
}

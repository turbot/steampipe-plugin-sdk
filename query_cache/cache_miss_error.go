package query_cache

type CacheMissError struct{}

func (CacheMissError) Error() string { return "cache miss" }

func IsCacheMiss(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == CacheMissError{}.Error()
}

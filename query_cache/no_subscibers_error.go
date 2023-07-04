package query_cache

type NoSubscribersError struct{}

func (NoSubscribersError) Error() string { return "set request has no subscribers" }

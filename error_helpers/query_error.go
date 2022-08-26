package error_helpers

// QueryError is an error type to wrap query errors
type QueryError struct {
	underlying error
}

func NewQueryError(underlying error) QueryError {
	return QueryError{underlying}
}

func (q QueryError) Error() string {
	return q.underlying.Error()
}

func (q QueryError) Is(err error) bool {
	_, isQueryError := err.(QueryError)
	return isQueryError
}

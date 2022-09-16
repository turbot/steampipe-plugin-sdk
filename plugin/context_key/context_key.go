// Package context_key provides keys used to retrieve items from the context
package context_key

// https://medium.com/@matryer/context-keys-in-go-5312346a868d
type contextKey string

func (c contextKey) String() string {
	return "plugin context key " + string(c)
}

var (
	Logger     = contextKey("logger")
	MatrixItem = contextKey("fetch_metadata")
)

package proto

func NewQueryContext(columns []string, qualMap map[string]*Quals, limit int64) *QueryContext {
	var queryContext = &QueryContext{
		Columns: columns,
		Quals:   qualMap,
	}
	if limit != -1 {
		queryContext.Limit = &NullableInt{Value: limit}
	}

	return queryContext
}

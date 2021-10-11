package cache

import "time"

type QueryCacheResult struct {
	Rows          []map[string]interface{}
	InsertionTime time.Time
}

func (q *QueryCacheResult) Append(row map[string]interface{}) {
	q.Rows = append(q.Rows, row)
}

package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
)

/*
QueryContext contains key query properties:
  - the columns requested.
  - all quals specified (not just key column quals).
  - the limit.
  - is caching enabled.
  - cache TTl.
*/
type QueryContext struct {
	Columns      []string
	UnsafeQuals  map[string]*proto.Quals
	Limit        *int64
	CacheEnabled bool
	CacheTTL     int64
}

// NewQueryContext maps from a proto.QueryContext to a plugin.QueryContext.
func NewQueryContext(p *proto.QueryContext, limit *proto.NullableInt, cacheEnabled bool, cacheTTL int64) *QueryContext {
	q := &QueryContext{
		Columns:      p.Columns,
		UnsafeQuals:  p.Quals,
		CacheEnabled: cacheEnabled,
		CacheTTL:     cacheTTL,
	}
	if limit != nil {
		q.Limit = &limit.Value
	}
	return q
}

// GetLimit converts [plugin.QueryContext.Limit] from a *int64 to an int64 (where -1 means no limit)
func (q *QueryContext) GetLimit() int64 {
	var limit int64 = -1
	if q.Limit != nil {
		limit = *q.Limit
	}
	return limit
}

// for count(*) queries, there will be no columns - add in 1 column so that we have some data to return
func (q *QueryContext) ensureColumns(table *Table) {
	if len(q.Columns) != 0 {
		return
	}

	var col string
	for _, c := range table.Columns {
		if c.Hydrate == nil {
			col = c.Name
			break
		}
	}
	if col == "" {
		// all columns have hydrate - just return the first column
		col = table.Columns[0].Name
	}
	// set queryContext.Columns to be this single column
	q.Columns = []string{col}
}

package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
)

/*
QueryContext contains important query properties:

  - The columns requested.

  - All quals specified (not just key column quals).

  - The limit.

  - Cache properties.
*/
type QueryContext struct {
	Columns      []string
	UnsafeQuals  map[string]*proto.Quals
	Limit        *int64
	CacheEnabled bool
	CacheTTL     int64
	SortOrder 	   []*proto.SortColumn
}

// NewQueryContext maps from a [proto.QueryContext] to a [plugin.QueryContext].
func NewQueryContext(p *proto.QueryContext, limit *proto.NullableInt, cacheEnabled bool, cacheTTL int64, table *Table) *QueryContext {
	q := &QueryContext{
		UnsafeQuals:  p.Quals,
		CacheEnabled: cacheEnabled,
		CacheTTL:     cacheTTL,
		SortOrder:    p.SortOrder,
	}
	if limit != nil {
		q.Limit = &limit.Value
	}
	// set columns
	// NOTE: only set columns which are supported by this table
	// (in the case of dynamic aggregators, the query may request
	// columns that this table does not provide for this connection)
	for _, c := range p.Columns {
		// context column is not in the table column map
		if _, hasColumn := table.columnNameMap[c]; hasColumn || c == deprecatedContextColumnName {
			q.Columns = append(q.Columns, c)
		}
	}

	if len(q.SortOrder) > 0 {
		log.Printf("[INFO] Sort order pushed down:  (%d), %v:", len(q.SortOrder), q.SortOrder)
	}
	return q
}

// GetLimit converts [plugin.QueryContext.Limit] from a *int64 to an int64 (where -1 means no limit).
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

package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
)

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

// GetLimit converts limit from *int64 to an int64 (where -1 means no limit)
func (q QueryContext) GetLimit() int64 {
	var limit int64 = -1
	if q.Limit != nil {
		limit = *q.Limit
	}
	return limit
}

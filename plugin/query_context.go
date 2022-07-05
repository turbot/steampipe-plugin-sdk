package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
)

type QueryContext struct {
	Columns     []string
	UnsafeQuals map[string]*proto.Quals
	Limit       *int64
}

// NewQueryContext maps from a proto.QueryContext to a plugin.QueryContext.
// the only difference is the representation of the limit (as protobuf does not support pointers)
func NewQueryContext(p *proto.QueryContext) *QueryContext {
	q := &QueryContext{
		Columns:     p.Columns,
		UnsafeQuals: p.Quals,
	}
	if p.Limit != nil {
		q.Limit = &p.Limit.Value
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

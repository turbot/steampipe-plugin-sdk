package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

type QueryContext struct {
	Columns  []string
	RawQuals map[string]*proto.Quals
	Limit    *int64
}

// NewQueryContext maps from a proto.QueryContext to a plugin.QueryContext.
// the only difference is the representation of the limit (as protobuf does not support pointers)
func NewQueryContext(p *proto.QueryContext) *QueryContext {
	q := &QueryContext{
		Columns:  p.Columns,
		RawQuals: p.Quals,
	}
	if p.Limit != nil {
		q.Limit = &p.Limit.Value
	}
	return q
}

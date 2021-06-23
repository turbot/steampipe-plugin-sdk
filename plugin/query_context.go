package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

type QueryContext struct {
	Columns  []string
	RawQuals map[string]*proto.Quals
	Limit    *int64
	Offset   *int64
}

func NewQueryContext(p *proto.QueryContext) *QueryContext {
	q := &QueryContext{
		Columns:  p.Columns,
		RawQuals: p.Quals,
	}
	if p.Limit != nil {
		q.Limit = &p.Limit.Value
	}
	if p.Offset != nil {
		q.Offset = &p.Offset.Value
	}
	return q
}

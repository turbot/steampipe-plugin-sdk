package cache

import (
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
)

type QueryCacheResult struct {
	Rows []*proto.Row
}

func (r *QueryCacheResult) Append(row *proto.Row) {
	r.Rows = append(r.Rows, row)
}

func (r *QueryCacheResult) AsProto() *proto.QueryResult {
	return &proto.QueryResult{
		Rows: r.Rows,
	}

}

func QueryCacheResultFromProto(r *proto.QueryResult) *QueryCacheResult {
	return &QueryCacheResult{
		Rows: r.Rows,
	}
}

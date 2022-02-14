package cache

import (
	"github.com/turbot/steampipe-plugin-sdk/v2/grpc/proto"
)

type QueryCacheResult struct {
	Rows []*proto.Row
}

func (q *QueryCacheResult) Append(row *proto.Row) {
	q.Rows = append(q.Rows, row)
}

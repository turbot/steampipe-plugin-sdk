package cache

import (
	"time"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

type QueryCacheResult struct {
	Rows          []*proto.Row
	InsertionTime time.Time
}

func (q *QueryCacheResult) Append(row *proto.Row) {
	q.Rows = append(q.Rows, row)
}

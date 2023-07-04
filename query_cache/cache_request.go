package query_cache

import (
	"context"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"time"
)

type CacheRequest struct {
	CallId         string
	Table          string
	QualMap        map[string]*sdkproto.Quals
	Columns        []string
	Limit          int64
	ConnectionName string
	TtlSeconds     int64

	resultKeyRoot string
	pageCount     int64
	rowCount      int
	StreamContext context.Context
}

func (req *CacheRequest) ttl() time.Duration {
	return time.Duration(req.TtlSeconds) * time.Second
}

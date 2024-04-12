package query_cache

import (
	"context"
	"fmt"
	"strings"
	"time"

	sdkproto "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

type CacheRequest struct {
	CallId         string
	Table          string
	QualMap        map[string]*sdkproto.Quals
	Columns        []string
	Limit          int64
	ConnectionName string
	TtlSeconds     int64
	SortOrder      []*sdkproto.SortColumn

	resultKeyRoot string
	pageCount     int64
	rowCount      int
	StreamContext context.Context
}

func (req *CacheRequest) ttl() time.Duration {
	return time.Duration(req.TtlSeconds) * time.Second
}

func (req *CacheRequest) sortOrderString() string {
	var strs []string
	for _, sortColumn := range req.SortOrder {
		strs = append(strs, fmt.Sprintf("%s(%s)", sortColumn.Column, sortColumn.Order))
	}
	return strings.Join(strs, "_")
}

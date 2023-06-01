package query_cache

import (
	"fmt"
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
	resultKeyRoot  string

	// used for set requests
	rows      []*sdkproto.Row
	raws      []any
	rowIndex  int
	pageCount int64
	err       error
}

func (req *CacheRequest) ttl() time.Duration {
	return time.Duration(req.TtlSeconds) * time.Second
}

// get result key for the most recent page of the request
func (req *CacheRequest) getPageResultKey() string {
	return getPageKey(req.resultKeyRoot, req.pageCount-1)
}

func (req *CacheRequest) getPrevPageResultKeys() []string {
	var res []string
	for i := 0; i < int(req.pageCount); i++ {
		res = append(res, getPageKey(req.resultKeyRoot, req.pageCount-1))
	}
	return res
}

func (req *CacheRequest) getRows() []*sdkproto.Row {
	if req.rowIndex == 0 {
		return nil
	}
	return req.rows[:req.rowIndex]
}

func getPageKey(resultKeyRoot string, pageIdx int64) string {
	return fmt.Sprintf("%s-%d", resultKeyRoot, pageIdx)
}

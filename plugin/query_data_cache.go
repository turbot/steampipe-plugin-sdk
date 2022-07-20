package plugin

import (
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"sort"
)

//func (d *QueryData) streamRowToCache(row *proto.Row) {
//	// if this is the first row to stream, we need to start the set operation
//	if d.cacheResultKey == "" {
//		d.startCacheSet(row)
//	} else {
//		d.iterateCacheSet(row)
//	}
//}
//
//func (d *QueryData) startCacheSet(row *proto.Row) {
//	if !d.cacheEnabled {
//		return
//	}
//
//	// ensure our local cache is empty
//	d.cacheRows = nil
//
//	// (we cannot do it before now as we need to examine the columns from the first row returned to
//	// build the key)
//	// so cache is enabled but the data is not in the cache
//	d.cacheColumns = d.buildColumnsFromRow(row, d.QueryContext.Columns)
//	resultKey, err := d.plugin.queryCache.StartSet(row, d.Table.Name, d.Quals.ToProtoQualMap(), d.cacheColumns, d.QueryContext.GetLimit(), d.callId, d.Connection.Name)
//	if err != nil {
//		// if StartSet failed, just log the error but continue
//		log.Printf("[WARN] queryCache.StartSet failed - this query will not be cached: %s", err.Error())
//		// disable caching for this scan
//		d.cacheEnabled = false
//	} else {
//		// save callId to querydata
//		d.cacheResultKey = resultKey
//	}
//}

// inspect the result row to build a full list of columns
func (d *QueryData) buildColumnsFromRow(row *proto.Row, columns []string) []string {
	for col := range row.Columns {
		if !helpers.StringSliceContains(columns, col) {
			columns = append(columns, col)
		}
	}
	sort.Strings(columns)
	return columns
}

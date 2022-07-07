package plugin

import (
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"log"
	"sort"
)

func (d *QueryData) streamRowToCache(row *proto.Row) {
	// if this is the first row to stream, we need to start the set operation
	if d.cacheResultKey == "" {
		d.startCacheSet(row)
	} else {
		d.iterateCacheSet(row)
	}
}

func (d *QueryData) startCacheSet(row *proto.Row) {
	if !d.cacheEnabled {
		return
	}

	// ensure our local cache is empty
	d.cacheRows = nil

	// (we cannot do it before now as we need to examine the columns from the first row returned to
	// build the key)
	// so cache is enabled but the data is not in the cache
	d.cacheColumns = d.buildColumnsFromRow(row, d.QueryContext.Columns)
	resultKey, err := d.plugin.queryCache.StartSet(row, d.Table.Name, d.Quals.ToProtoQualMap(), d.cacheColumns, d.QueryContext.GetLimit(), d.callId)
	if err != nil {
		// if StartSet failed, just log the error but continue
		log.Printf("[WARN] queryCache.StartSet failed - this query will not be cached: %s", err.Error())
		// disable caching for this scan
		d.cacheEnabled = false
	} else {
		// save callId to querydata
		d.cacheResultKey = resultKey
	}
}

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

func (d *QueryData) iterateCacheSet(row *proto.Row) {
	if !d.cacheEnabled {
		return
	}

	// we buffer rows and send in chunks
	const cacheRowChunkSize = 1000
	if len(d.cacheRows) < cacheRowChunkSize {
		d.cacheRows = append(d.cacheRows, row)
	} else {
		d.plugin.queryCache.IterateSet(d.cacheRows, d.callId)
		d.cacheRows = nil
	}
}

func (d *QueryData) endCacheSet() {
	if !d.cacheEnabled {
		return
	}

	// send any remaining rows
	d.plugin.queryCache.EndSet(d.cacheRows, d.Table.Name, d.QueryContext.UnsafeQuals, d.cacheColumns, d.QueryContext.GetLimit(), d.callId, d.cacheResultKey)
	d.cacheRows = nil
}

func (d *QueryData) abortCacheSet() {
	if !d.cacheEnabled {
		return
	}
	d.plugin.queryCache.AbortSet(d.callId)
	d.cacheRows = nil
}

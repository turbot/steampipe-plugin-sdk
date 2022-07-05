package plugin

import (
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"log"
)

func (d *QueryData) streamRowToCache(row *proto.Row) {
	// if this is the first row to stream, we need to start the set operation
	if d.cacheSetCallId == "" {
		d.startCacheSet(row)
	} else {
		d.iterateCacheSet(row)
	}
}

func (d *QueryData) startCacheSet(row *proto.Row) {
	if !d.cacheEnabled {
		return
	}

	// (we cannot do it before now as we need to examine the columns from the first row returned to
	// build the key)
	// so cache is enabled but the data is not in the cache
	cacheSetCallId, err := d.plugin.queryCache.StartSet(row, d.Table.Name, d.Quals.ToProtoQualMap(), d.QueryContext.Columns, d.QueryContext.GetLimit())
	if err != nil {
		// if StartSet failed, just log the error but continue
		log.Printf("[WARN] queryCache.StartSet failed - this query will not be cached: %s", err.Error())
		// disable caching for this scan
		d.cacheEnabled = false
	} else {
		// save callId to querydata
		d.cacheSetCallId = cacheSetCallId
	}
}

func (d *QueryData) iterateCacheSet(row *proto.Row) {
	if !d.cacheEnabled {
		return
	}
	d.plugin.queryCache.IterateSet(row, d.cacheSetCallId)
}

func (d *QueryData) endCacheSet() {
	if !d.cacheEnabled {
		return
	}
	d.plugin.queryCache.EndSet(d.Table.Name, d.QueryContext.UnsafeQuals, d.QueryContext.Columns, d.QueryContext.GetLimit(), d.cacheSetCallId)
}

func (d *QueryData) abortCacheSet() {
	if !d.cacheEnabled {
		return
	}
	d.plugin.queryCache.AbortSet(d.cacheSetCallId)
}

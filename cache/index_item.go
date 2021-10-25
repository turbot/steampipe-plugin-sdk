package cache

import (
	"log"

	"github.com/turbot/go-kit/helpers"
)

// QueryCacheIndexBucket contains index items for all cache results for a given table and qual set
type QueryCacheIndexBucket struct {
	Items []*IndexItem
}

func newIndexBucket() *QueryCacheIndexBucket {
	return &QueryCacheIndexBucket{}
}

func (b *QueryCacheIndexBucket) Append(item *IndexItem) *QueryCacheIndexBucket {
	b.Items = append(b.Items, item)
	return b
}

// Get finds an index item which satisfies all columns
func (b *QueryCacheIndexBucket) Get(columns []string, limit int64) *IndexItem {
	for _, item := range b.Items {
		if item.SatisfiesColumns(columns) && item.SatisfiesLimit(limit) {
			return item
		}
	}
	return nil
}

// IndexItem stores the columns and cached index for a single cached query result
// note - this index item it tied to a specific table and set of quals
type IndexItem struct {
	Columns []string
	Key     string
	Limit   int64
}

func NewIndexItem(columns []string, key string, limit int64) *IndexItem {
	return &IndexItem{
		Columns: columns,
		Key:     key,
		Limit:   limit,
	}
}

func (i IndexItem) SatisfiesColumns(columns []string) bool {
	for _, c := range columns {
		if !helpers.StringSliceContains(i.Columns, c) {
			return false
		}
	}
	return true
}

func (i IndexItem) SatisfiesLimit(limit int64) bool {
	// if index item has is no limit, it will be -1
	if i.Limit == -1 {
		log.Printf("[TRACE] SatisfiesLimit limit %d, no item limit - satisfied", limit)
		return true
	}
	log.Printf("[TRACE] SatisfiesLimit limit %d, item limit %d ", limit, i.Limit)
	// if 'limit' is -1 and i.Limit is not, we cannot satisfy this
	if limit == -1 {
		return false
	}
	// otherwise just check whether limit is <= item limit>
	res := limit <= i.Limit
	log.Printf("[TRACE] satisfied = %v", res)
	return res

}

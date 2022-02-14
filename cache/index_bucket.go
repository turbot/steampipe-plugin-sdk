package cache

import (
	"log"

	"github.com/turbot/steampipe-plugin-sdk/v2/grpc/proto"
)

// IndexBucket contains index items for all cache results for a given table and qual set
type IndexBucket struct {
	Items []*IndexItem
}

func newIndexBucket() *IndexBucket {
	return &IndexBucket{}
}

func (b *IndexBucket) Append(item *IndexItem) *IndexBucket {
	b.Items = append(b.Items, item)
	return b
}

// Get finds an index item which satisfies all columns
func (b *IndexBucket) Get(qualMap map[string]*proto.Quals, columns []string, limit, ttlSeconds int64) *IndexItem {
	for _, item := range b.Items {
		log.Printf("[TRACE] IndexBucket.Get key %s limit %d", item.Key, item.Limit)
		if item.SatisfiesQuals(qualMap) && item.SatisfiesColumns(columns) && item.SatisfiesLimit(limit) && item.SatisfiesTtl(ttlSeconds) {
			return item
		}
	}
	return nil
}

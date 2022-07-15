package query_cache

import (
	"log"

	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
func (b *IndexBucket) Get(qualMap map[string]*proto.Quals, columns []string, limit, ttlSeconds int64, keyColumns map[string]*proto.KeyColumn) *IndexItem {
	for _, item := range b.Items {
		log.Printf("[TRACE] IndexBucket.Get key %s limit %d", item.Key, item.Limit)
		if item.SatisfiesQuals(qualMap, keyColumns) && item.SatisfiesColumns(columns) && item.SatisfiesLimit(limit) && item.SatisfiesTtl(ttlSeconds) {
			return item
		}
	}
	return nil
}

func (b *IndexBucket) AsProto() *proto.IndexBucket {
	res := &proto.IndexBucket{
		Items: make([]*proto.IndexItem, len(b.Items)),
	}
	for i, item := range b.Items {
		res.Items[i] = &proto.IndexItem{
			Key:           item.Key,
			Quals:         item.Quals,
			Columns:       item.Columns,
			Limit:         item.Limit,
			InsertionTime: timestamppb.New(item.InsertionTime),
		}
	}
	return res
}

func IndexBucketfromProto(b *proto.IndexBucket) *IndexBucket {
	res := &IndexBucket{
		Items: make([]*IndexItem, len(b.Items)),
	}
	for i, item := range b.Items {
		res.Items[i] = &IndexItem{
			Key:           item.Key,
			Quals:         item.Quals,
			Columns:       item.Columns,
			Limit:         item.Limit,
			InsertionTime: item.InsertionTime.AsTime(),
		}
	}
	return res
}

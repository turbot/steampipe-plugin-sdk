package query_cache

import (
	"log"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// IndexBucket contains index items for all cache results for a given table and connection
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
func (b *IndexBucket) Get(req *CacheRequest, keyColumns map[string]*proto.KeyColumn) *IndexItem {
	log.Printf("[INFO] IndexBucket.Get %d items", len(b.Items))
	for _, item := range b.Items {
		//log.Printf("[INFO] IndexBucket.Get key %s limit %d (%s)", item.Key, item.Limit, req.CallId)
		if item.satisfiesRequest(req.Columns, req.Limit, req.QualMap, keyColumns) && item.satisfiesTtl(req.TtlSeconds) {
			//log.Printf("[INFO] IndexBucket.Get CACHE HIT %d items", len(b.Items))
			return item
		}
	}
	//log.Printf("[WARN] IndexBucket.Get CACHE MISS %d items", len(b.Items))
	//log.Printf("[WARN] req QUALS: %s", grpc.QualMapToLogLine(req.QualMap))
	//for _, item := range b.Items {
	//	log.Printf("[WARN] item QUALS: %s", grpc.QualMapToLogLine(item.Quals))
	//}

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
			PageCount:     item.PageCount,
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
			PageCount:     item.PageCount,
			InsertionTime: item.InsertionTime.AsTime(),
		}
	}
	return res
}

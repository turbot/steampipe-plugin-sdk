package query_cache

import (
	"fmt"
	"log"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// pendingIndexBucket contains index items for all pending cache results for a given table and qual set
// (keyed by the root result key)
type pendingIndexBucket struct {
	Items map[string]*pendingIndexItem
}

func newPendingIndexBucket() *pendingIndexBucket {
	return &pendingIndexBucket{Items: make(map[string]*pendingIndexItem)}
}

// GetItemsSatisfyingRequest finds all index item which satisfy the given cache request
// used when finding a pending item after a cache miss occurs
func (b *pendingIndexBucket) GetItemsSatisfyingRequest(req *CacheRequest, keyColumns map[string]*proto.KeyColumn) []*pendingIndexItem {
	var satisfyingItems []*pendingIndexItem

	for _, pendingItem := range b.Items {
		if pendingItem.SatisfiesRequest(req, keyColumns) {
			qualsString := grpc.QualMapToLogLine(req.QualMap)

			log.Printf("[TRACE] found pending index item to satisfy columns %s, limit %d, quals: %s (%s)", strings.Join(req.Columns, ","), req.Limit, qualsString, req.CallId)
			satisfyingItems = append(satisfyingItems, pendingItem)
		}
	}
	return satisfyingItems
}

// GetItemsSatisfiedByRequest finds all index item which would be SATISFIED BY  the given cache request
// used when finding a pending items to mark as complete after a cache set has been executed
func (b *pendingIndexBucket) GetItemsSatisfiedByRequest(req *CacheRequest, keyColumns map[string]*proto.KeyColumn) []*pendingIndexItem {
	var satisfyingItems []*pendingIndexItem

	for _, pendingItem := range b.Items {
		if pendingItem.SatisfiedByRequest(req, keyColumns) {
			qualsString := grpc.QualMapToLogLine(req.QualMap)

			log.Printf("[TRACE] found pending index item satisfied by columns %s, limit %d, quals: %s (%s)", strings.Join(req.Columns, ","), req.Limit, qualsString, req.CallId)
			satisfyingItems = append(satisfyingItems, pendingItem)
		}
	}
	return satisfyingItems
}

func (b *pendingIndexBucket) delete(pendingItem *pendingIndexItem) {
	delete(b.Items, pendingItem.item.Key)
}

func (b *pendingIndexBucket) String() any {
	var sb strings.Builder
	for itemKey, item := range b.Items {
		sb.WriteString(fmt.Sprintf("item: %p, key:%s\n", item, itemKey))
	}
	return sb.String()
}

// pendingIndexItem stores the columns and cached index for a single pending query result
// note - this index item it tied to a specific table and set of quals
type pendingIndexItem struct {
	item              *IndexItem
	err               error
	callId            string
	pendingSetRequest *setRequest
}

func NewPendingIndexItem(pendingSetRequest *setRequest) *pendingIndexItem {
	return &pendingIndexItem{
		item:              NewIndexItem(pendingSetRequest.CacheRequest),
		callId:            pendingSetRequest.CallId,
		pendingSetRequest: pendingSetRequest,
	}
}

// SatisfiesRequest returns whether our index item satisfies the given cache request
func (i *pendingIndexItem) SatisfiesRequest(req *CacheRequest, keyColumns map[string]*proto.KeyColumn) bool {
	return i.item.SatisfiesRequest(req.Columns, req.Limit, req.QualMap, keyColumns)
}

// SatisfiedByRequest returns whether our index item would be satisfied by the given cache request
func (i *pendingIndexItem) SatisfiedByRequest(req *CacheRequest, keyColumns map[string]*proto.KeyColumn) bool {
	return i.item.SatisfiedByRequest(req, keyColumns)
}

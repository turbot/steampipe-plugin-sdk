package query_cache

import (
	"fmt"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"golang.org/x/exp/maps"
	"log"
	"strings"
	"sync"
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
			qualsString := strings.Join(maps.Keys(req.QualMap), ",")
			if qualsString == "" {
				qualsString = "NONE"
			}

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
			qualsString := strings.Join(maps.Keys(req.QualMap), ",")
			if qualsString == "" {
				qualsString = "NONE"
			}

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
		sb.WriteString(fmt.Sprintf("item: %p, count: %d, key:%s\n", item, item.count, itemKey))
	}
	return sb.String()
}

// pendingIndexItem stores the columns and cached index for a single pending query result
// note - this index item it tied to a specific table and set of quals
type pendingIndexItem struct {
	item *IndexItem
	wg   *sync.WaitGroup
	// used for logging purposes only (as we cannot access wait groups count)
	count int
	err   error
}

func (i *pendingIndexItem) Lock() {
	log.Printf("[TRACE] pendingIndexItem Lock count before %d", i.count)
	i.wg.Add(1)
	i.count++
}

func (i *pendingIndexItem) Unlock(err error) {
	i.err = err
	log.Printf("[TRACE] pendingIndexItem Unlock count before %d key %s", i.count, i.item.Key)
	i.wg.Done()
	i.count--
}

func (i *pendingIndexItem) Wait() error {
	log.Printf("[TRACE] pendingIndexItem Wait %p, %s", i, i.item.Key)

	i.wg.Wait()
	log.Printf("[TRACE] pendingIndexItem Wait DONE %p, %s, err: %v", i, i.item.Key, i.err)
	return i.err
}

func NewPendingIndexItem(req *CacheRequest) *pendingIndexItem {
	res := &pendingIndexItem{
		item: NewIndexItem(req),
		wg:   new(sync.WaitGroup),
	}
	// increment wait group - indicate this item is pending
	res.Lock()
	return res
}

// SatisfiesRequest returns whether our index item satisfies the given cache request
func (i *pendingIndexItem) SatisfiesRequest(req *CacheRequest, keyColumns map[string]*proto.KeyColumn) bool {
	return i.item.SatisfiesRequest(req.Columns, req.Limit, req.QualMap, keyColumns)
}

// SatisfiedByRequest returns whether our index item would be satisfied by the given cache request
func (i *pendingIndexItem) SatisfiedByRequest(req *CacheRequest, keyColumns map[string]*proto.KeyColumn) bool {
	return i.item.SatisfiedByRequest(req, keyColumns)
}

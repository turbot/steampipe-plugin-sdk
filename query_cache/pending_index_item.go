package query_cache

import (
	"fmt"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"log"
	"strings"
	"sync"

	"github.com/turbot/go-kit/helpers"
)

// pendingIndexBucket contains index items for all pending cache results for a given table and qual set
// (keyed by the root result key)
type pendingIndexBucket struct {
	Items map[string]*pendingIndexItem
}

func newPendingIndexBucket() *pendingIndexBucket {
	return &pendingIndexBucket{Items: make(map[string]*pendingIndexItem)}
}

// GetItemsSatisfyingRequest finds all index item which are satisfied by the column
// used when removing pending IndexItems after a cache Set call
func (b *pendingIndexBucket) GetItemsSatisfyingRequest(req *CacheRequest, keyColumns map[string]*proto.KeyColumn) []*pendingIndexItem {
	var satisfiedItems []*pendingIndexItem
	columns := req.Columns
	limit := req.Limit
	quals := req.QualMap
	for _, item := range b.Items {
		if item.SatisfiedByColumns(columns) && item.SatisfiesLimit(limit) && item.SatisfiesQuals(quals, keyColumns) {
			log.Printf("[INFO] found pending index item to satisfy columns %s, limit %d, quals: %s", strings.Join(columns, ","), limit, grpc.QualMapToString(quals, true))
			satisfiedItems = append(satisfiedItems, item)
		}
	}
	return satisfiedItems
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

// SatisfiesColumns returns whether our index item satisfies the given columns
func (i *pendingIndexItem) SatisfiesColumns(columns []string) bool {
	return i.item.SatisfiesColumns(columns)
}

// SatisfiesLimit returns whether our index item satisfies the given limit
func (i *pendingIndexItem) SatisfiesLimit(limit int64) bool {
	return i.item.SatisfiesLimit(limit)
}

// SatisfiesQuals returns whether our index item satisfies the given quals
func (i *pendingIndexItem) SatisfiesQuals(qualMap map[string]*proto.Quals, keyColumns map[string]*proto.KeyColumn) bool {
	return i.item.SatisfiesQuals(qualMap, keyColumns)
}

// SatisfiedByColumns returns whether we would be satisfied by the given columns
func (i *pendingIndexItem) SatisfiedByColumns(columns []string) bool {
	// does columns contain all out index item columns?
	for _, c := range i.item.Columns {
		if !helpers.StringSliceContains(columns, c) {
			log.Printf("[TRACE] SatisfiedByColumns %s missing from %s", c, strings.Join(columns, ","))
			return false
		}
	}
	return true
}

// SatisfiedByLimit returns whether we would be satisfied by the given limt
func (i *pendingIndexItem) SatisfiedByLimit(limit int64) bool {
	// if index item has no limit, we would only be satisfied by no limit
	if i.item.Limit == -1 {
		satisfied := limit == -1
		log.Printf("[TRACE] SatisfiedByLimit limit %d, no item limit - satisfied: %v", limit, satisfied)
		return satisfied
	}
	log.Printf("[TRACE] SatisfiesLimit limit %d, item limit %d ", limit, i.item.Limit)
	// if 'limit' is -1 - it must satisfy us
	if limit == -1 {
		log.Printf("[TRACE] SatisfiedByLimit - no limit so it satisfied")
		return true
	}

	// otherwise just check whether limit is >= item limit
	res := limit >= i.item.Limit
	log.Printf("[TRACE] satisfied = %v", res)
	return res
}

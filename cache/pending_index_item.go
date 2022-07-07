package cache

import (
	"log"
	"strings"
	"sync"

	"github.com/turbot/go-kit/helpers"
)

// pendingIndexBucket contains index items for all pending cache results for a given table and qual set
type pendingIndexBucket struct {
	Items map[string]*pendingIndexItem
}

func newPendingIndexBucket() *pendingIndexBucket {
	return &pendingIndexBucket{Items: make(map[string]*pendingIndexItem)}
}

// GetItemWhichSatisfiesColumnsAndLimit finds an index item which satisfies all columns
// used to find an IndexItem to satisfy a cache Get request
func (b *pendingIndexBucket) GetItemWhichSatisfiesColumnsAndLimit(columns []string, limit int64) *pendingIndexItem {
	log.Printf("[TRACE] find pending index item to satisfy columns %v and limit %d", columns, limit)
	for _, item := range b.Items {
		if item.SatisfiesColumns(columns) && item.SatisfiesLimit(limit) {
			log.Printf("[TRACE] found pending index item to satisfy columns %s, limit %d", strings.Join(columns, ","), limit)
			return item
		}
	}
	return nil
}

// GetItemsSatisfiedByColumns finds all index item which are satisfied by the columsn and limit columns
// used when removing pending IndexItems after a cache Set call
func (b *pendingIndexBucket) GetItemsSatisfiedByColumns(columns []string, limit int64) []*pendingIndexItem {
	var satisfiedItems []*pendingIndexItem

	for _, item := range b.Items {
		if item.SatisfiedByColumns(columns) && item.SatisfiesLimit(limit) {
			log.Printf("[TRACE] found pending index item to satisfy columns %s, limit %d", strings.Join(columns, ","), limit)
			satisfiedItems = append(satisfiedItems, item)
		}
	}
	return satisfiedItems
}

func (b *pendingIndexBucket) delete(pendingItem *pendingIndexItem) {
	delete(b.Items, pendingItem.item.Key)
}

// pendingIndexItem stores the columns and cached index for a single pending query result
// note - this index item it tied to a specific table and set of quals
type pendingIndexItem struct {
	item *IndexItem
	lock *sync.WaitGroup
	// used for logging purposes only (as we cannot access wait groups count)
	count int
}

func (p *pendingIndexItem) Lock() {
	log.Printf("[TRACE] pendingIndexItem Lock count before %d", p.count)
	p.lock.Add(1)
	p.count++
}

func (p *pendingIndexItem) Unlock() {
	log.Printf("[TRACE] pendingIndexItem Unlock count before %d", p.count)
	p.lock.Done()
	p.count--
}

func (p *pendingIndexItem) Wait() {
	log.Printf("[TRACE] pendingIndexItem Wait")

	p.lock.Wait()
	log.Printf("[TRACE] pendingIndexItem Wait DONE")
}

func NewPendingIndexItem(columns []string, key string, limit int64) *pendingIndexItem {
	res := &pendingIndexItem{
		item: NewIndexItem(columns, key, limit, nil),
		lock: new(sync.WaitGroup),
	}
	// increment wait group - indicate this item is pending
	res.Lock()
	return res
}

// SatisfiesColumns returns whether our index item satisfies the given columns
func (i pendingIndexItem) SatisfiesColumns(columns []string) bool {
	return i.item.SatisfiesColumns(columns)
}

// SatisfiesLimit returns whether our index item satisfies the given limit
func (i pendingIndexItem) SatisfiesLimit(limit int64) bool {
	return i.item.SatisfiesLimit(limit)
}

// SatisfiedByColumns returns whether we would be satisfied by the given columns
func (i pendingIndexItem) SatisfiedByColumns(columns []string) bool {
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
func (i pendingIndexItem) SatisfiedByLimit(limit int64) bool {
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

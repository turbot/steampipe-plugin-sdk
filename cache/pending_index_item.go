package cache

import (
	"log"
	"sync"
)

// pendingIndexBucket contains index items for all pending cache results for a given table and qual set
type pendingIndexBucket struct {
	Items map[string]*pendingIndexItem
}

func newPendingIndexBucket() *pendingIndexBucket {
	return &pendingIndexBucket{Items: make(map[string]*pendingIndexItem)}
}

// Get finds an index item which satisfies all columns
func (b *pendingIndexBucket) Get(columns []string, limit int64) *pendingIndexItem {
	for _, item := range b.Items {
		if item.SatisfiesColumns(columns) && item.SatisfiesLimit(limit) {
			log.Printf("[TRACE] found pending index item to satisfy columns %v and limit %d", columns, limit)
			return item
		}
	}
	return nil
}

func (b *pendingIndexBucket) delete(pendingItem *pendingIndexItem) {
	delete(b.Items, pendingItem.item.Key)
}

// pendingIndexItem stores the columns and cached index for a single pending query result
// note - this index item it tied to a specific table and set of quals
type pendingIndexItem struct {
	item  *IndexItem
	lock  *sync.WaitGroup
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
		item: NewIndexItem(columns, key, limit),
		lock: new(sync.WaitGroup),
	}
	// increment wait group - indicate this item is pending
	res.Lock()
	return res
}

func (i pendingIndexItem) SatisfiesColumns(columns []string) bool {
	return i.item.SatisfiesColumns(columns)
}

func (i pendingIndexItem) SatisfiesLimit(limit int64) bool {
	return i.item.SatisfiesLimit(limit)
}

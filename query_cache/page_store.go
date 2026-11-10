package query_cache

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

// entryOverheadBytes estimates what each entry costs beyond its key and value:
// the map entry, the list element and the entry struct.
const entryOverheadBytes = 200

// pageStore keeps the query cache entries (result pages and index buckets) in memory.
//
//   - It counts the bytes of every entry against the limit, and removes the oldest
//     entries first to make room.
//   - It removes each entry maxTtl after it was written.
//   - Removed entries are freed, so their memory goes back to the Go runtime.
//
// Every entry has the same lifetime, so the oldest entry is also the first to expire.
// One list in write order serves for both eviction and expiry.
type pageStore struct {
	mu     sync.Mutex
	limit  int64 // bytes; 0 means no limit
	maxTtl time.Duration
	size   int64
	// key -> element of order
	entries map[string]*list.Element
	// *storeEntry, oldest first
	order *list.List
	// tag -> keys with that tag
	tagKeys map[string]map[string]struct{}
	// removes expired entries when nothing else touches the store
	sweep *time.Timer
}

type storeEntry struct {
	key     string
	value   []byte
	tags    []string
	expires time.Time
}

func (e *storeEntry) cost() int64 {
	return int64(len(e.key) + len(e.value) + entryOverheadBytes)
}

func newPageStore(limitMb int, maxTtl time.Duration) *pageStore {
	return &pageStore{
		limit:   int64(max(limitMb, 0)) * 1024 * 1024,
		maxTtl:  maxTtl,
		entries: make(map[string]*list.Element),
		order:   list.New(),
		tagKeys: make(map[string]map[string]struct{}),
	}
}

// get returns the value stored for key. The caller must not change the returned bytes.
func (s *pageStore) get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.removeExpiredLocked()
	el, ok := s.entries[key]
	if !ok {
		return nil, false
	}
	return el.Value.(*storeEntry).value, true
}

// set stores value for key, replacing any value already there.
// The store keeps value, so the caller must not change it afterwards.
func (s *pageStore) set(key string, value []byte, tags []string) error {
	e := &storeEntry{key: key, value: value, tags: tags}
	if s.limit > 0 && e.cost() > s.limit {
		return fmt.Errorf("cache entry of %d bytes is bigger than the cache limit of %d bytes", e.cost(), s.limit)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if el, ok := s.entries[key]; ok {
		s.removeLocked(el)
	}
	s.removeExpiredLocked()
	s.evictLocked(e.cost())

	e.expires = time.Now().Add(s.maxTtl)
	s.entries[key] = s.order.PushBack(e)
	s.size += e.cost()
	for _, tag := range tags {
		keys, ok := s.tagKeys[tag]
		if !ok {
			keys = make(map[string]struct{})
			s.tagKeys[tag] = keys
		}
		keys[key] = struct{}{}
	}
	s.scheduleSweepLocked()
	return nil
}

func (s *pageStore) delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if el, ok := s.entries[key]; ok {
		s.removeLocked(el)
	}
}

// invalidateTag removes every entry stored with tag.
func (s *pageStore) invalidateTag(tag string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key := range s.tagKeys[tag] {
		s.removeLocked(s.entries[key])
	}
}

// removeLocked removes one entry. s.mu must be held.
func (s *pageStore) removeLocked(el *list.Element) {
	e := s.order.Remove(el).(*storeEntry)
	delete(s.entries, e.key)
	s.size -= e.cost()
	for _, tag := range e.tags {
		keys := s.tagKeys[tag]
		delete(keys, e.key)
		if len(keys) == 0 {
			delete(s.tagKeys, tag)
		}
	}
}

// removeExpiredLocked removes the entries whose lifetime has passed. s.mu must be held.
func (s *pageStore) removeExpiredLocked() {
	now := time.Now()
	for el := s.order.Front(); el != nil && !now.Before(el.Value.(*storeEntry).expires); el = s.order.Front() {
		s.removeLocked(el)
	}
}

// evictLocked removes the oldest entries until there is room for needed more bytes.
// s.mu must be held.
func (s *pageStore) evictLocked(needed int64) {
	if s.limit == 0 {
		return
	}
	for s.size+needed > s.limit && s.order.Len() > 0 {
		s.removeLocked(s.order.Front())
	}
}

// scheduleSweepLocked starts a timer for when the oldest entry expires, unless one is
// already running. s.mu must be held.
func (s *pageStore) scheduleSweepLocked() {
	if s.sweep != nil {
		return
	}
	front := s.order.Front()
	if front == nil {
		return
	}
	s.sweep = time.AfterFunc(time.Until(front.Value.(*storeEntry).expires), s.runSweep)
}

func (s *pageStore) runSweep() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sweep = nil
	s.removeExpiredLocked()
	s.scheduleSweepLocked()
}

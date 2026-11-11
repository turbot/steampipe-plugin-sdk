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
//   - A reader that still needs an entry pins it. The store does not evict, expire or
//     delete a pinned entry until the last pin is released, so the store can go over its
//     limit while readers are behind.
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
	// the number of readers that still need this entry
	pins int
	// set when the entry is deleted, or is bigger than the limit, while it is pinned:
	// the last unpin removes it
	dropped bool
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

// set stores value for key, replacing any value already there, and pins it pins times.
// A value bigger than the limit is kept only for its pins: it is removed when the last
// pin is released, and set returns an error.
// The store keeps value, so the caller must not change it afterwards.
func (s *pageStore) set(key string, value []byte, tags []string, pins int) error {
	e := &storeEntry{key: key, value: value, tags: tags, pins: pins}
	var err error
	if s.limit > 0 && e.cost() > s.limit {
		err = fmt.Errorf("cache entry of %d bytes is bigger than the cache limit of %d bytes", e.cost(), s.limit)
		if pins == 0 {
			return err
		}
		e.dropped = true
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
	return err
}

// pin pins the entries for all of keys, or for none of them if one of them is not stored,
// has expired or is dropped. It returns whether it pinned them.
func (s *pageStore) pin(keys []string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for _, key := range keys {
		el, ok := s.entries[key]
		if !ok {
			return false
		}
		if e := el.Value.(*storeEntry); e.dropped || !now.Before(e.expires) {
			return false
		}
	}
	for _, key := range keys {
		s.entries[key].Value.(*storeEntry).pins++
	}
	return true
}

// unpin releases one pin on the entry for each of keys. Then the store removes the entries
// that are now unpinned and dropped or expired, and evicts entries if it is over its limit.
func (s *pageStore) unpin(keys ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, key := range keys {
		el, ok := s.entries[key]
		if !ok {
			continue
		}
		e := el.Value.(*storeEntry)
		e.pins--
		if e.pins == 0 && e.dropped {
			s.removeLocked(el)
		}
	}
	s.removeExpiredLocked()
	s.evictLocked(0)
}

// delete removes the entry for key. A pinned entry is removed when its last pin is released.
func (s *pageStore) delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if el, ok := s.entries[key]; ok {
		s.dropLocked(el)
	}
}

// invalidateTag removes every entry stored with tag. A pinned entry is removed when its
// last pin is released.
func (s *pageStore) invalidateTag(tag string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key := range s.tagKeys[tag] {
		s.dropLocked(s.entries[key])
	}
}

// dropLocked removes an entry now, or marks it to be removed when its last pin is released.
// s.mu must be held.
func (s *pageStore) dropLocked(el *list.Element) {
	if e := el.Value.(*storeEntry); e.pins > 0 {
		e.dropped = true
		return
	}
	s.removeLocked(el)
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

// removeExpiredLocked removes the unpinned entries whose lifetime has passed. s.mu must be held.
func (s *pageStore) removeExpiredLocked() {
	now := time.Now()
	for el := s.order.Front(); el != nil && !now.Before(el.Value.(*storeEntry).expires); {
		next := el.Next()
		if el.Value.(*storeEntry).pins == 0 {
			s.removeLocked(el)
		}
		el = next
	}
}

// evictLocked removes the oldest unpinned entries until there is room for needed more bytes,
// or no unpinned entry is left. s.mu must be held.
func (s *pageStore) evictLocked(needed int64) {
	if s.limit == 0 {
		return
	}
	for el := s.order.Front(); el != nil && s.size+needed > s.limit; {
		next := el.Next()
		if el.Value.(*storeEntry).pins == 0 {
			s.removeLocked(el)
		}
		el = next
	}
}

// scheduleSweepLocked starts a timer for when the oldest entry that has not expired expires,
// unless one is already running. An expired entry that is still there is pinned: the last
// unpin removes it. s.mu must be held.
func (s *pageStore) scheduleSweepLocked() {
	if s.sweep != nil {
		return
	}
	now := time.Now()
	for el := s.order.Front(); el != nil; el = el.Next() {
		if expires := el.Value.(*storeEntry).expires; now.Before(expires) {
			s.sweep = time.AfterFunc(expires.Sub(now), s.runSweep)
			return
		}
	}
}

func (s *pageStore) runSweep() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sweep = nil
	s.removeExpiredLocked()
	s.scheduleSweepLocked()
}

package query_cache

import (
	"testing"
	"time"
)

func entryPins(s *pageStore, key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.entries[key]; ok {
		return el.Value.(*storeEntry).pins
	}
	return -1
}

// The store evicts unpinned entries first, and goes over its limit rather than evict a pinned
// entry. When the pin is released, the store evicts again.
func TestPageStore_PinnedEntryIsNotEvicted(t *testing.T) {
	s := newPageStore(1, time.Hour)
	if err := s.set("pinned", kb(400), nil, 1); err != nil {
		t.Fatal(err)
	}
	mustSet(t, s, "a", kb(400))
	mustSet(t, s, "b", kb(400))
	checkInvariants(t, s)

	// "a" is evicted to make room for "b"; "pinned" is older, but pinned
	if _, ok := s.get("a"); ok {
		t.Fatal("unpinned entry a was kept, and the pinned entry was evicted")
	}
	if _, ok := s.get("pinned"); !ok {
		t.Fatal("the pinned entry was evicted")
	}

	mustSet(t, s, "c", kb(700))
	checkInvariants(t, s)
	if size, _ := storeSize(s); size <= s.limit {
		t.Fatalf("size %d is within the limit: the test needs the pinned entry to keep the store over it", size)
	}

	s.unpin("pinned")
	checkInvariants(t, s)
	if _, ok := s.get("pinned"); ok {
		t.Fatal("the entry is still stored after its pin was released, with the store over its limit")
	}
	if size, _ := storeSize(s); size > s.limit {
		t.Fatalf("size %d is over the limit %d after the pin was released", size, s.limit)
	}
}

// A pinned entry does not expire. The last unpin removes it if it has expired.
func TestPageStore_PinnedEntryDoesNotExpire(t *testing.T) {
	const ttl = 50 * time.Millisecond
	s := newPageStore(0, ttl)
	if err := s.set("pinned", kb(1), nil, 2); err != nil {
		t.Fatal(err)
	}
	mustSet(t, s, "unpinned", kb(1))
	time.Sleep(3 * ttl)

	if _, ok := s.get("unpinned"); ok {
		t.Fatal("the unpinned entry did not expire")
	}
	if _, ok := s.get("pinned"); !ok {
		t.Fatal("the pinned entry expired")
	}
	if s.pin([]string{"pinned"}) {
		t.Fatal("pin succeeded for an expired entry")
	}

	s.unpin("pinned")
	if _, ok := s.get("pinned"); !ok {
		t.Fatal("the entry was removed while it still had a pin")
	}
	s.unpin("pinned")
	if size, n := storeSize(s); n != 0 || size != 0 {
		t.Fatalf("after the last unpin of an expired entry the store holds %d entries, %d bytes", n, size)
	}
	checkInvariants(t, s)
}

// Deleting or invalidating a pinned entry stops new readers from pinning it. Its readers
// can still get it, and the last unpin removes it.
func TestPageStore_DeletedPinnedEntryIsRemovedOnLastUnpin(t *testing.T) {
	for name, remove := range map[string]func(s *pageStore){
		"delete":        func(s *pageStore) { s.delete("pinned") },
		"invalidateTag": func(s *pageStore) { s.invalidateTag("conn") },
	} {
		t.Run(name, func(t *testing.T) {
			s := newPageStore(0, time.Hour)
			if err := s.set("pinned", kb(1), []string{"conn"}, 1); err != nil {
				t.Fatal(err)
			}
			remove(s)
			checkInvariants(t, s)

			if _, ok := s.get("pinned"); !ok {
				t.Fatal("the entry was removed while it still had a pin")
			}
			if s.pin([]string{"pinned"}) {
				t.Fatal("pin succeeded for a deleted entry")
			}
			s.unpin("pinned")
			if _, ok := s.get("pinned"); ok {
				t.Fatal("the deleted entry is still stored after its last unpin")
			}
			checkInvariants(t, s)
		})
	}
}

// pin pins all the keys, or none of them if one is missing.
func TestPageStore_PinIsAllOrNothing(t *testing.T) {
	s := newPageStore(0, time.Hour)
	mustSet(t, s, "a", kb(1))
	mustSet(t, s, "b", kb(1))

	if s.pin([]string{"a", "missing", "b"}) {
		t.Fatal("pin succeeded with a missing key")
	}
	if pins := entryPins(s, "a") + entryPins(s, "b"); pins != 0 {
		t.Fatalf("a failed pin left %d pins", pins)
	}
	if !s.pin([]string{"a", "b"}) {
		t.Fatal("pin failed for stored entries")
	}
	if entryPins(s, "a") != 1 || entryPins(s, "b") != 1 {
		t.Fatalf("pins are a=%d b=%d, want 1 each", entryPins(s, "a"), entryPins(s, "b"))
	}
	s.unpin("a", "b")
	checkInvariants(t, s)
}

// An entry bigger than the limit is not stored, unless it is pinned: then it is kept for
// its readers, and removed when they release it.
func TestPageStore_EntryBiggerThanLimitIsKeptOnlyForItsPins(t *testing.T) {
	s := newPageStore(1, time.Hour)
	mustSet(t, s, "small", kb(100))

	if err := s.set("big", kb(2048), nil, 1); err == nil {
		t.Fatal("set returned no error for an entry bigger than the limit")
	}
	if _, ok := s.get("big"); !ok {
		t.Fatal("the pinned entry bigger than the limit is not stored")
	}
	if s.pin([]string{"big"}) {
		t.Fatal("pin succeeded for an entry bigger than the limit")
	}
	checkInvariants(t, s)

	s.unpin("big")
	if _, ok := s.get("big"); ok {
		t.Fatal("the entry bigger than the limit is still stored after its last unpin")
	}
	checkInvariants(t, s)
}

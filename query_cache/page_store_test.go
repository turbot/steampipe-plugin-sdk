package query_cache

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func kb(n int) []byte { return bytes.Repeat([]byte("x"), n*1024) }

// checkInvariants verifies the store's own bookkeeping.
func checkInvariants(t *testing.T, s *pageStore) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()

	var size int64
	for el := s.order.Front(); el != nil; el = el.Next() {
		e := el.Value.(*storeEntry)
		size += e.cost()
		if e.pins < 0 {
			t.Fatalf("entry %q has %d pins", e.key, e.pins)
		}
		if e.dropped && e.pins == 0 {
			t.Fatalf("entry %q is dropped and unpinned, but still stored", e.key)
		}
		// over the limit, the store keeps only pinned entries, and the entry it stored last
		if s.limit > 0 && s.size > s.limit && e.pins == 0 && el != s.order.Back() {
			t.Fatalf("size %d is over the limit %d, but unpinned entry %q is still stored", s.size, s.limit, e.key)
		}
		if s.entries[e.key] != el {
			t.Fatalf("entry %q is in the order list but not in the map", e.key)
		}
		for _, tag := range e.tags {
			if _, ok := s.tagKeys[tag][e.key]; !ok {
				t.Fatalf("entry %q is missing from tag %q", e.key, tag)
			}
		}
	}
	if size != s.size {
		t.Fatalf("size is %d, entries add up to %d", s.size, size)
	}
	if len(s.entries) != s.order.Len() {
		t.Fatalf("map has %d entries, list has %d", len(s.entries), s.order.Len())
	}
	for tag, keys := range s.tagKeys {
		for key := range keys {
			if _, ok := s.entries[key]; !ok {
				t.Fatalf("tag %q lists %q, which is not stored", tag, key)
			}
		}
	}
}

func storeSize(s *pageStore) (int64, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size, len(s.entries)
}

func mustSet(t *testing.T, s *pageStore, key string, value []byte, tags ...string) {
	t.Helper()
	if err := s.set(key, value, tags, 0); err != nil {
		t.Fatalf("set %q: %v", key, err)
	}
}

func TestPageStore_SetGet(t *testing.T) {
	s := newPageStore(10, time.Hour)
	mustSet(t, s, "a", []byte("hello"))

	got, ok := s.get("a")
	if !ok || string(got) != "hello" {
		t.Fatalf("get a = %q, %v; want hello, true", got, ok)
	}
	if _, ok := s.get("missing"); ok {
		t.Fatal("get of a missing key returned a value")
	}
	checkInvariants(t, s)
}

func TestPageStore_ReplaceCountsOnlyTheNewValue(t *testing.T) {
	s := newPageStore(10, time.Hour)
	mustSet(t, s, "a", kb(100))
	mustSet(t, s, "a", kb(50))

	size, n := storeSize(s)
	want := (&storeEntry{key: "a", value: kb(50)}).cost()
	if n != 1 || size != want {
		t.Fatalf("after replacing, %d entries of %d bytes; want 1 of %d", n, size, want)
	}
	checkInvariants(t, s)
}

func TestPageStore_EvictsOldestFirst(t *testing.T) {
	s := newPageStore(1, time.Hour) // 1 MiB
	for _, k := range []string{"a", "b", "c"} {
		mustSet(t, s, k, kb(300))
	}
	// a 4th entry does not fit: the oldest goes
	mustSet(t, s, "d", kb(300))

	if _, ok := s.get("a"); ok {
		t.Error("a is the oldest entry and should have been evicted")
	}
	for _, k := range []string{"b", "c", "d"} {
		if _, ok := s.get(k); !ok {
			t.Errorf("%s should still be cached", k)
		}
	}
	checkInvariants(t, s)
}

func TestPageStore_RejectsEntryBiggerThanLimit(t *testing.T) {
	s := newPageStore(1, time.Hour)
	mustSet(t, s, "small", kb(10))

	if err := s.set("big", kb(2048), nil, 0); err == nil {
		t.Fatal("an entry bigger than the whole limit was accepted")
	}
	// the store does not empty itself trying to make room
	if _, ok := s.get("small"); !ok {
		t.Fatal("a rejected entry evicted an existing one")
	}
	checkInvariants(t, s)
}

func TestPageStore_NoLimit(t *testing.T) {
	s := newPageStore(0, time.Hour)
	for i := 0; i < 20; i++ {
		mustSet(t, s, fmt.Sprintf("k%d", i), kb(1024))
	}
	if _, n := storeSize(s); n != 20 {
		t.Fatalf("with no limit, %d of 20 entries are stored", n)
	}
	checkInvariants(t, s)
}

func TestPageStore_EntriesExpireAfterMaxTtl(t *testing.T) {
	s := newPageStore(10, 50*time.Millisecond)
	mustSet(t, s, "a", []byte("v"))
	if _, ok := s.get("a"); !ok {
		t.Fatal("a fresh entry is missing")
	}
	time.Sleep(80 * time.Millisecond)
	if _, ok := s.get("a"); ok {
		t.Fatal("an entry older than the max TTL was returned")
	}
	checkInvariants(t, s)
}

// Expired entries are freed even when nothing reads or writes the cache,
// so an idle plugin gives the memory back.
func TestPageStore_SweepFreesExpiredEntriesWhenIdle(t *testing.T) {
	s := newPageStore(10, 30*time.Millisecond)
	for i := 0; i < 3; i++ {
		mustSet(t, s, fmt.Sprintf("k%d", i), kb(100))
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if size, n := storeSize(s); size == 0 && n == 0 {
			break
		}
		if time.Now().After(deadline) {
			size, n := storeSize(s)
			t.Fatalf("2 s after expiry the idle store still holds %d entries, %d bytes", n, size)
		}
		time.Sleep(10 * time.Millisecond)
	}
	checkInvariants(t, s)
}

func TestPageStore_InvalidateTag(t *testing.T) {
	s := newPageStore(10, time.Hour)
	mustSet(t, s, "a1", []byte("v"), "conn_a")
	mustSet(t, s, "a2", []byte("v"), "conn_a")
	mustSet(t, s, "b1", []byte("v"), "conn_b")

	s.invalidateTag("conn_a")

	for _, k := range []string{"a1", "a2"} {
		if _, ok := s.get(k); ok {
			t.Errorf("%s has tag conn_a and should have been removed", k)
		}
	}
	if _, ok := s.get("b1"); !ok {
		t.Error("b1 has tag conn_b and should still be cached")
	}
	checkInvariants(t, s)
}

// The cached-result reader checks only page 0 before it starts streaming
// (cacher_result_subscriber.go), assuming that if page 0 is there, every later
// page is too. With one oldest-first order over the whole store that holds: a
// result's pages are written in order, so page 0 always goes first.
func TestPageStore_LaterPagesOutliveFirstPage(t *testing.T) {
	s := newPageStore(2, time.Hour)
	const pages = 4
	for p := 0; p < pages; p++ {
		mustSet(t, s, getPageKey("result_a", p), kb(200))
	}
	for i := 0; i < 20; i++ {
		mustSet(t, s, getPageKey("result_b", i), kb(200))

		if _, ok := s.get(getPageKey("result_a", 0)); !ok {
			continue
		}
		for p := 1; p < pages; p++ {
			if _, ok := s.get(getPageKey("result_a", p)); !ok {
				t.Fatalf("after %d writes, page 0 of result_a is cached but page %d is not", i+1, p)
			}
		}
	}
	for p := 0; p < pages; p++ {
		if _, ok := s.get(getPageKey("result_a", p)); ok {
			t.Fatalf("page %d of result_a should have been evicted by now", p)
		}
	}
	checkInvariants(t, s)
}

func TestPageStore_ConcurrentUse(t *testing.T) {
	s := newPageStore(1, time.Hour)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed)) //nolint:gosec // G404: a seeded sequence makes the test repeatable; not used for security
			for i := 0; i < 2000; i++ {
				key := fmt.Sprintf("k%d", r.Intn(50))
				tag := fmt.Sprintf("t%d", r.Intn(3))
				switch r.Intn(10) {
				case 0:
					s.delete(key)
				case 1:
					s.invalidateTag(tag)
				case 2, 3, 4:
					_ = s.set(key, kb(r.Intn(64)+1), []string{tag}, 0)
				default:
					s.get(key)
				}
			}
		}(int64(g))
	}
	wg.Wait()
	checkInvariants(t, s)
}

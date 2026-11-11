package query_cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	sdkproto "github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
)

// These tests run scans through the query cache as the plugin does: a query that misses
// the cache starts a scan (Get), the scan adds its rows (IterateSet), then ends (EndSet).
// Each query reads its rows with a testReader.

// testWait is how long a test waits for a scan or a query before it reports that it hangs.
const testWait = 30 * time.Second

// testReader is a query that reads rows from the cache. It records the id of each row it gets.
// It can stop after pauseAt rows, until resume is closed.
type testReader struct {
	mu      sync.Mutex
	ids     []string
	pauseAt int
	paused  chan struct{}
	resume  chan struct{}
}

// newTestReader returns a reader that stops after pauseAt rows. If pauseAt is 0, it does not stop.
func newTestReader(pauseAt int) *testReader {
	return &testReader{pauseAt: pauseAt, paused: make(chan struct{}), resume: make(chan struct{})}
}

func (r *testReader) stream(row *sdkproto.Row) {
	r.mu.Lock()
	r.ids = append(r.ids, row.Columns["id"].GetStringValue())
	pause := len(r.ids) == r.pauseAt
	r.mu.Unlock()
	if pause {
		close(r.paused)
		<-r.resume
	}
}

func (r *testReader) rowCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ids)
}

// checkRows checks that the reader got each row of the named scan once, and no other rows.
func (r *testReader) checkRows(t *testing.T, who, scan string, rows int) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	counts := make(map[string]int, len(r.ids))
	for _, id := range r.ids {
		counts[id]++
	}
	missing, twice := 0, 0
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("%s-%d", scan, i)
		switch counts[id] {
		case 0:
			missing++
		case 1:
		default:
			twice++
		}
		delete(counts, id)
	}
	if missing > 0 || twice > 0 || len(counts) > 0 {
		t.Errorf("%s got %d rows for the %d rows of %s: %d missing, %d more than once, %d from another scan",
			who, len(r.ids), rows, scan, missing, twice, len(counts))
	}
}

func scanRow(scan string, i, rowBytes int) *sdkproto.Row {
	return &sdkproto.Row{Columns: map[string]*sdkproto.Column{
		"id":   {Value: &sdkproto.Column_StringValue{StringValue: fmt.Sprintf("%s-%d", scan, i)}},
		"data": {Value: &sdkproto.Column_StringValue{StringValue: strings.Repeat("x", rowBytes)}},
	}}
}

// newQueryRequest makes a request for the one query that all these tests use.
// A cached result satisfies the request only if it is younger than ttlSeconds.
func newQueryRequest(callId string, ttlSeconds int64) *CacheRequest {
	return &CacheRequest{
		CallId:         callId,
		Table:          "t",
		Columns:        []string{"id", "data"},
		Limit:          -1,
		ConnectionName: "conn",
		TtlSeconds:     ttlSeconds,
		StreamContext:  context.Background(),
	}
}

// startQuery starts a query in the background. The channel gets the result of its Get.
func startQuery(c *QueryCache, callId string, ttlSeconds int64, r *testReader) chan error {
	done := make(chan error, 1)
	go func() {
		done <- c.Get(context.Background(), newQueryRequest(callId, ttlSeconds), r.stream, r.stream)
	}()
	return done
}

// waitFor waits for a result from done, and fails the test if it does not come within testWait.
func waitFor(t *testing.T, what string, done chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(testWait):
		t.Fatalf("%s did not return within %s", what, testWait)
		return nil
	}
}

func waitUntil(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(testWait)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting until %s", testWait, what)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitUntilPaused(t *testing.T, who string, r *testReader) {
	t.Helper()
	select {
	case <-r.paused:
	case <-time.After(testWait):
		t.Fatalf("%s did not get %d rows within %s; it has %d", who, r.pauseAt, testWait, r.rowCount())
	}
}

// testScan is a scan that a query started when it missed the cache.
type testScan struct {
	t        *testing.T
	c        *QueryCache
	callId   string
	rowBytes int
	rows     int
}

// startTestScan makes a query that misses the cache, so it starts a scan.
// own is the query's reader: the scan's own reader.
func startTestScan(t *testing.T, c *QueryCache, callId string, ttlSeconds int64, rowBytes int, own *testReader) *testScan {
	t.Helper()
	if err := c.Get(context.Background(), newQueryRequest(callId, ttlSeconds), own.stream, own.stream); !IsCacheMiss(err) {
		t.Fatalf("%s: Get returned %v, want a cache miss that starts a scan", callId, err)
	}
	return &testScan{t: t, c: c, callId: callId, rowBytes: rowBytes}
}

func (s *testScan) request() *setRequest {
	s.c.setRequestMapLock.RLock()
	defer s.c.setRequestMapLock.RUnlock()
	return s.c.setRequests[s.callId]
}

func (s *testScan) readers() int {
	req := s.request()
	req.requestLock.RLock()
	defer req.requestLock.RUnlock()
	return len(req.subscribers)
}

// write adds n more rows to the scan, as the plugin does.
func (s *testScan) write(n int) error {
	s.t.Helper()
	done := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			if err := s.c.IterateSet(context.Background(), scanRow(s.callId, s.rows, s.rowBytes), s.callId); err != nil {
				done <- fmt.Errorf("IterateSet row %d: %w", s.rows, err)
				return
			}
			s.rows++
		}
		done <- nil
	}()
	return waitFor(s.t, fmt.Sprintf("%s: writing %d rows", s.callId, n), done)
}

// end ends the scan in the background. EndSet waits for the scan's readers to finish.
func (s *testScan) end() chan error {
	done := make(chan error, 1)
	go func() { done <- s.c.EndSet(context.Background(), s.callId) }()
	return done
}

// A query that joins a running scan reads the scan's earlier pages from the cache. While
// it is behind, the scan writes more than the cache limit. The query still gets every row.
func TestQueryCache_LateReaderGetsEveryRowWhenTheScanFillsTheCache(t *testing.T) {
	c := newTestQueryCache(t, 3, time.Hour)
	own := newTestReader(0)
	scan := startTestScan(t, c, "scan", 3600, 1000, own)
	if err := scan.write(2 * rowBufferSize); err != nil {
		t.Fatal(err)
	}

	late := newTestReader(1)
	lateDone := startQuery(c, "late", 3600, late)
	waitUntil(t, "the late query joins the scan", func() bool { return scan.readers() == 2 })
	waitUntilPaused(t, "late query", late)

	// pages of about 1 MB: 8 more pages through a 3 MB cache
	if err := scan.write(8 * rowBufferSize); err != nil {
		t.Fatal(err)
	}
	ended := scan.end()
	close(late.resume)

	if err := waitFor(t, "late query", lateDone); err != nil {
		t.Errorf("late query failed after %d rows: %v", late.rowCount(), err)
	}
	if err := waitFor(t, "EndSet", ended); err != nil {
		t.Errorf("EndSet: %v", err)
	}
	late.checkRows(t, "late query", "scan", scan.rows)
	own.checkRows(t, "scan's own query", "scan", scan.rows)
}

// The query that started a scan is a reader of the scan too. It stops for a while, and the scan
// writes more than the cache limit and ends. The query still gets every row.
func TestQueryCache_ScanOwnReaderGetsEveryRowWhenTheScanFillsTheCache(t *testing.T) {
	c := newTestQueryCache(t, 3, time.Hour)
	own := newTestReader(500)
	scan := startTestScan(t, c, "scan", 3600, 1000, own)
	if err := scan.write(rowBufferSize); err != nil {
		t.Fatal(err)
	}
	waitUntilPaused(t, "scan's own query", own)

	if err := scan.write(9 * rowBufferSize); err != nil {
		t.Fatal(err)
	}
	ended := scan.end()
	close(own.resume)

	if err := waitFor(t, "EndSet", ended); err != nil {
		t.Errorf("EndSet: %v", err)
	}
	own.checkRows(t, "scan's own query", "scan", scan.rows)
}

// If the scan's own reader cannot read a page, its query must fail. A query that ends
// with no error and only part of its rows is a wrong result.
func TestQueryCache_ScanFailsWhenItsOwnReaderLosesAPage(t *testing.T) {
	c := newTestQueryCache(t, 0, time.Hour)
	own := newTestReader(1)
	scan := startTestScan(t, c, "scan", 3600, 100, own)
	if err := scan.write(3 * rowBufferSize); err != nil {
		t.Fatal(err)
	}
	waitUntilPaused(t, "scan's own query", own)

	// remove page 1 from the store, as if the store had lost it
	pageKey := getPageKey(scan.request().resultKeyRoot, 1)
	c.cache.mu.Lock()
	if el, ok := c.cache.entries[pageKey]; ok {
		c.cache.removeLocked(el)
	} else {
		t.Fatalf("page 1 (%s) is not in the store", pageKey)
	}
	c.cache.mu.Unlock()

	ended := scan.end()
	close(own.resume)

	if err := waitFor(t, "EndSet", ended); err == nil {
		t.Fatalf("EndSet returned no error, but the scan's own query lost page 1 and got %d of %d rows", own.rowCount(), scan.rows)
	}
}

// A page bigger than the cache limit cannot be cached. The query must still get every row,
// with no error, and the result must not be cached.
func TestQueryCache_PageBiggerThanTheCacheDoesNotFailTheQuery(t *testing.T) {
	c := newTestQueryCache(t, 1, time.Hour)
	own := newTestReader(0)
	// pages of about 1.5 MB
	scan := startTestScan(t, c, "scan", 3600, 1500, own)
	if err := scan.write(2*rowBufferSize + rowBufferSize/2); err != nil {
		t.Fatal(err)
	}
	if err := waitFor(t, "EndSet", scan.end()); err != nil {
		t.Errorf("EndSet: %v", err)
	}
	own.checkRows(t, "scan's own query", "scan", scan.rows)

	again := newTestReader(0)
	err := waitFor(t, "second query", startQuery(c, "again", 3600, again))
	if !IsCacheMiss(err) || again.rowCount() != 0 {
		t.Errorf("second query: got %d rows and error %v, want a cache miss with no rows", again.rowCount(), err)
	}
	c.AbortSet(context.Background(), "again", errors.New("test done"))
}

// A query that reads a cached result must get all of it, even if the cache evicts the result's
// pages while the query is part way through. A cache miss after some rows were sent makes the
// plugin fetch the whole result again, so the query gets those rows twice.
func TestQueryCache_CacheHitGetsWholeResultWhenItsPagesAreEvicted(t *testing.T) {
	c := newTestQueryCache(t, 5, time.Hour)
	own := newTestReader(0)
	scan := startTestScan(t, c, "scan", 3600, 1000, own)
	if err := scan.write(3*rowBufferSize + rowBufferSize/2); err != nil {
		t.Fatal(err)
	}
	if err := waitFor(t, "EndSet", scan.end()); err != nil {
		t.Fatalf("EndSet: %v", err)
	}

	hit := newTestReader(500)
	hitDone := startQuery(c, "hit", 3600, hit)
	waitUntilPaused(t, "cache hit query", hit)

	// 6 MB of other results through the 5 MB cache
	for i := 0; i < 3; i++ {
		writeTestResult(t, c, fmt.Sprintf("other_%d", i), 2*rowBufferSize, 1000)
	}
	close(hit.resume)

	if err := waitFor(t, "cache hit query", hitDone); err != nil {
		t.Errorf("cache hit query got %d rows, then error %v", hit.rowCount(), err)
		c.AbortSet(context.Background(), "hit", errors.New("test done"))
	}
	hit.checkRows(t, "cache hit query", "scan", scan.rows)
}

// A new scan of a query must not change a cached result of the same query that a query is reading.
func TestQueryCache_NewScanDoesNotChangeACachedResultBeingRead(t *testing.T) {
	c := newTestQueryCache(t, 0, time.Hour)
	own1 := newTestReader(0)
	scan1 := startTestScan(t, c, "scan1", 3600, 100, own1)
	if err := scan1.write(3*rowBufferSize + rowBufferSize/2); err != nil {
		t.Fatal(err)
	}
	if err := waitFor(t, "EndSet scan1", scan1.end()); err != nil {
		t.Fatalf("EndSet scan1: %v", err)
	}

	hit := newTestReader(500)
	hitDone := startQuery(c, "hit", 3600, hit)
	waitUntilPaused(t, "cache hit query", hit)

	// a query that accepts no cached result (ttl 0) starts a second scan of the same query
	own2 := newTestReader(0)
	scan2 := startTestScan(t, c, "scan2", 0, 100, own2)
	if err := scan2.write(3*rowBufferSize + rowBufferSize/2); err != nil {
		t.Fatal(err)
	}
	if err := waitFor(t, "EndSet scan2", scan2.end()); err != nil {
		t.Fatalf("EndSet scan2: %v", err)
	}
	close(hit.resume)

	if err := waitFor(t, "cache hit query", hitDone); err != nil {
		t.Errorf("cache hit query: %v", err)
	}
	hit.checkRows(t, "cache hit query", "scan1", scan1.rows)
}

// A query that joins a running scan whose first pages the cache no longer has cannot read
// those rows. It must get a cache miss, with no rows, so the plugin scans for it.
func TestQueryCache_LateQueryScansAgainWhenTheScansFirstPageIsGone(t *testing.T) {
	c := newTestQueryCache(t, 3, time.Hour)
	own := newTestReader(0)
	scan := startTestScan(t, c, "scan", 3600, 1000, own)
	if err := scan.write(6 * rowBufferSize); err != nil {
		t.Fatal(err)
	}
	// the scan's own query has read every page, so the cache can evict the first pages
	waitUntil(t, "the scan's own query has read 6 pages", func() bool { return own.rowCount() == scan.rows })

	late := newTestReader(0)
	err := waitFor(t, "late query", startQuery(c, "late", 3600, late))
	if !IsCacheMiss(err) || late.rowCount() != 0 {
		t.Errorf("late query: got %d rows and error %v, want a cache miss with no rows", late.rowCount(), err)
	}
	c.AbortSet(context.Background(), "late", errors.New("test done"))

	if err := waitFor(t, "EndSet", scan.end()); err != nil {
		t.Errorf("EndSet: %v", err)
	}
	own.checkRows(t, "scan's own query", "scan", scan.rows)
}

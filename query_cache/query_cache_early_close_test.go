package query_cache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	sdkproto "github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
)

// These tests close a query's reader early, as the FDW does when Postgres rescans a table or stops
// after a LIMIT: the query's stream context is cancelled while the scan it reads goes on.

// earlyCloseWait is how long a test waits to show that something does not happen.
const earlyCloseWait = 200 * time.Millisecond

// startScanWithContext makes a query that misses the cache, so it starts a scan.
// The scan's own query reads with the given stream context.
func startScanWithContext(t *testing.T, c *QueryCache, callId string, streamCtx context.Context, own *testReader) *testScan {
	t.Helper()
	req := newQueryRequest(callId, 3600)
	req.StreamContext = streamCtx
	if err := c.Get(context.Background(), req, own.stream, own.stream); !IsCacheMiss(err) {
		t.Fatalf("%s: Get returned %v, want a cache miss that starts a scan", callId, err)
	}
	return &testScan{t: t, c: c, callId: callId, rowBytes: 100}
}

// When the query that started a scan closes, a query that joined the scan keeps it going and gets every row.
func TestQueryCache_ScanGoesOnWhenItsOwnQueryClosesAndAnotherJoined(t *testing.T) {
	c := newTestQueryCache(t, 64, time.Hour)
	ownCtx, closeOwn := context.WithCancel(context.Background())
	defer closeOwn()
	own := newTestReader(0)
	scan := startScanWithContext(t, c, "scan", ownCtx, own)
	if err := scan.write(rowBufferSize + 3); err != nil {
		t.Fatal(err)
	}

	// an identical query joins the running scan, as the next rescan does
	joined := newTestReader(0)
	joinedDone := startQuery(c, "joined", 3600, joined)
	waitUntil(t, "the second query has joined", func() bool { return scan.readers() == 2 })

	// the first query closes
	closeOwn()
	waitUntil(t, "the closed query has stopped reading", func() bool { return scan.readers() == 1 })

	// the scan is not cancelled: it writes more rows, over more pages, to the query that is left
	if err := scan.write(3 * rowBufferSize); err != nil {
		t.Fatalf("scan write after the first query closed: %v", err)
	}
	if err := waitFor(t, "EndSet", scan.end()); err != nil {
		t.Fatalf("EndSet: %v", err)
	}
	if err := waitFor(t, "joined query", joinedDone); err != nil {
		t.Fatalf("joined query: %v", err)
	}
	joined.checkRows(t, "joined query", "scan", scan.rows)
	checkNoPins(t, c)
}

// When the last query reading a scan closes, the scan is cancelled. A query that asks for the same data
// after that must not join the cancelled scan, and fail with it: it starts a scan of its own.
func TestQueryCache_QueryDoesNotJoinAScanThatHasNoReaders(t *testing.T) {
	c := newTestQueryCache(t, 64, time.Hour)
	ownCtx, closeOwn := context.WithCancel(context.Background())
	defer closeOwn()
	scan := startScanWithContext(t, c, "scan", ownCtx, newTestReader(0))
	if err := scan.write(5); err != nil {
		t.Fatal(err)
	}
	closeOwn()
	waitUntil(t, "the closed query has stopped reading", func() bool { return scan.readers() == 0 })

	// the scan's next row finds no readers: the plugin cancels the scan
	err := c.IterateSet(context.Background(), scanRow("scan", scan.rows, 100), "scan")
	var noSubscribers NoSubscribersError
	if !errors.As(err, &noSubscribers) {
		t.Fatalf("IterateSet with no readers returned %v, want NoSubscribersError", err)
	}

	// before the plugin has aborted the scan, the same query arrives
	late := newTestReader(0)
	lateDone := startQuery(c, "late", 3600, late)
	select {
	case err := <-lateDone:
		if !IsCacheMiss(err) {
			t.Fatalf("late query returned %v, want a cache miss that starts a scan", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("late query joined the scan that was being cancelled, and waits for rows it will never get")
	}

	c.AbortSet(context.Background(), "scan", err)

	// the late query's own scan is unaffected by the abort of the first
	lateScan := &testScan{t: t, c: c, callId: "late", rowBytes: 100}
	if err := lateScan.write(2 * rowBufferSize); err != nil {
		t.Fatalf("late query's scan: %v", err)
	}
	if err := waitFor(t, "EndSet late", lateScan.end()); err != nil {
		t.Fatalf("EndSet late: %v", err)
	}
	late.checkRows(t, "late query", "late", lateScan.rows)
	checkNoPins(t, c)
}

// The channel a query streams its rows to is closed once the query returns, so
// the cache must not stream a row for a query after the query's Get has returned.
func TestQueryCache_ClosedQueryStreamsNoRowOnceItsGetHasReturned(t *testing.T) {
	c := newTestQueryCache(t, 64, time.Hour)
	scan := startTestScan(t, c, "scan", 3600, 100, newTestReader(0))
	if err := scan.write(rowBufferSize + 3); err != nil {
		t.Fatal(err)
	}

	var getReturned atomic.Bool
	var late atomic.Int32
	paused := make(chan struct{})
	resume := make(chan struct{})
	streamed := make(chan struct{})
	var first atomic.Bool
	stream := func(row *sdkproto.Row) {
		if !first.CompareAndSwap(false, true) {
			return
		}
		close(paused)
		<-resume
		// the first row is still being streamed after the pause
		if getReturned.Load() {
			late.Add(1)
		}
		close(streamed)
	}

	closeCtx, closeQuery := context.WithCancel(context.Background())
	defer closeQuery()
	req := newQueryRequest("closing", 3600)
	req.StreamContext = closeCtx
	getDone := make(chan error, 1)
	getReturnedChan := make(chan struct{})
	go func() {
		err := c.Get(context.Background(), req, stream, stream)
		getReturned.Store(true)
		close(getReturnedChan)
		getDone <- err
	}()

	select {
	case <-paused:
	case <-time.After(testWait):
		t.Fatal("the query got no row")
	}
	closeQuery()
	// a Get that returns now would leave the query's row still being streamed
	select {
	case <-getReturnedChan:
	case <-time.After(earlyCloseWait):
	}
	close(resume)
	// the closed query's own Get error is not what this test checks, only that it has returned
	_ = waitFor(t, "closed query's Get", getDone)
	select {
	case <-streamed:
	case <-time.After(testWait):
		t.Fatal("the closed query's row was never streamed")
	}

	if n := late.Load(); n != 0 {
		t.Errorf("%d rows were still being streamed for the query after its Get returned", n)
	}
	if err := waitFor(t, "EndSet", scan.end()); err != nil {
		t.Errorf("EndSet: %v", err)
	}
	checkNoPins(t, c)
}

// When a scan is aborted, the query that started it must have stopped reading by the time AbortSet
// returns, as with EndSet: the plugin ends the query next, and nothing may stream a row for it after that.
func TestQueryCache_AbortSetWaitsForTheScansOwnQueryToStopReading(t *testing.T) {
	c := newTestQueryCache(t, 64, time.Hour)

	var abortReturned atomic.Bool
	var late atomic.Int32
	paused := make(chan struct{})
	resume := make(chan struct{})
	streamed := make(chan struct{})
	var first atomic.Bool
	stream := func(row *sdkproto.Row) {
		if !first.CompareAndSwap(false, true) {
			return
		}
		close(paused)
		<-resume
		// the first row is still being streamed after the pause
		if abortReturned.Load() {
			late.Add(1)
		}
		close(streamed)
	}
	if err := c.Get(context.Background(), newQueryRequest("scan", 3600), stream, stream); !IsCacheMiss(err) {
		t.Fatalf("Get returned %v, want a cache miss that starts a scan", err)
	}
	scan := &testScan{t: t, c: c, callId: "scan", rowBytes: 100}
	if err := scan.write(rowBufferSize); err != nil {
		t.Fatal(err)
	}
	select {
	case <-paused:
	case <-time.After(testWait):
		t.Fatal("the scan's own query got no row")
	}

	aborted := make(chan struct{})
	go func() {
		c.AbortSet(context.Background(), "scan", errors.New("scan failed"))
		abortReturned.Store(true)
		close(aborted)
	}()
	select {
	case <-aborted:
	case <-time.After(earlyCloseWait):
	}
	close(resume)
	select {
	case <-aborted:
	case <-time.After(testWait):
		t.Fatal("AbortSet did not return")
	}
	select {
	case <-streamed:
	case <-time.After(testWait):
		t.Fatal("the scan's own query's row was never streamed")
	}

	if n := late.Load(); n != 0 {
		t.Errorf("%d rows were still being streamed for the scan's own query after AbortSet returned", n)
	}
	checkNoPins(t, c)
}

package plugin

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConnectionConfig_Race exercises concurrent reads and writes of
// Connection.Config. Under `go test -race`, the race detector should
// flag the unsynchronized access in v6's commit 1 (accessors-without-lock)
// and pass cleanly after commit 2 adds the RWMutex.
//
// This is the regression test for the race surfaced during code review
// of steampipe-plugin-aws PR #2756.
func TestConnectionConfig_Race(t *testing.T) {
	c := &Connection{Name: "test"}
	c.SetConfig("initial")

	var stop atomic.Bool
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				_ = c.GetConfig()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for !stop.Load() {
			c.SetConfig(struct{ V int }{V: i})
			i++
		}
	}()

	time.Sleep(100 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}

// TestConnectionConfig_RotationPickup asserts the contract that GetConfig
// returns the most recently SetConfig value. Single-threaded — this is the
// functional contract, separate from the safety claim tested above.
func TestConnectionConfig_RotationPickup(t *testing.T) {
	c := &Connection{Name: "test"}

	c.SetConfig("first")
	if got := c.GetConfig(); got != "first" {
		t.Errorf("after SetConfig(\"first\"), GetConfig returned %v, want \"first\"", got)
	}

	c.SetConfig("second")
	if got := c.GetConfig(); got != "second" {
		t.Errorf("after SetConfig(\"second\"), GetConfig returned %v, want \"second\"", got)
	}
}

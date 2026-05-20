package plugin

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// raceTestDuration is how long TestConnectionConfig_Race lets the readers
// and writer interleave before stopping. 100ms is enough for the race
// detector to fire on a developer machine; CI runners under load can
// override with STEAMPIPE_SDK_RACE_TEST_DURATION=250ms (or any duration
// parseable by time.ParseDuration).
func raceTestDuration() time.Duration {
	const fallback = 100 * time.Millisecond
	v := os.Getenv("STEAMPIPE_SDK_RACE_TEST_DURATION")
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

// TestConnectionConfig_Race exercises concurrent reads and writes of the
// connection configuration through GetConfig/SetConfig. Regression test
// for a torn-read race when the SDK rotates credentials mid-query.
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

	time.Sleep(raceTestDuration())
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

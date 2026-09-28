package query_cache

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/grpc"
	sdkproto "github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
)

// cacheResultSubscriber.waitUntilDone used to call maxReadSem.Acquire and
// unconditionally spawn a goroutine that deferred maxReadSem.Release, even
// when Acquire failed to actually acquire a slot (e.g. because ctx was
// already cancelled). Releasing a semaphore slot that was never held panics
// ("semaphore: released more than held") once the running total goes
// negative - see golang.org/x/sync/semaphore's Release. That panic happens
// on a goroutine this package spawns internally, which a normal in-process
// recover() cannot catch from a test, so this runs the real scenario in a
// subprocess and checks whether it crashed.
func TestCacheResultSubscriberNoSemaphoreOverRelease(t *testing.T) {
	if os.Getenv("SDK_TEST_SEMAPHORE_OVER_RELEASE_SUBPROCESS") == "1" {
		runSemaphoreOverReleaseScenario(t)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestCacheResultSubscriberNoSemaphoreOverRelease", "-test.v") //nolint:gosec // re-invoking this same test binary; os.Args[0] is not attacker-controlled
	cmd.Env = append(os.Environ(), "SDK_TEST_SEMAPHORE_OVER_RELEASE_SUBPROCESS=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("subprocess exited with an error (semaphore over-release regression): %v\n%s", err, output)
	}
}

// runSemaphoreOverReleaseScenario drives waitUntilDone with an already-cancelled
// context and more than one page, which makes every maxReadSem.Acquire call in
// the loop fail immediately. On the buggy code this spawned a goroutine per
// page anyway, and every one of them released a slot it never acquired.
func runSemaphoreOverReleaseScenario(t *testing.T) {
	const pageCount = 3

	queryCache, err := NewQueryCache("test-plugin", map[string]*grpc.PluginSchema{}, &QueryCacheOptions{
		Enabled:   true,
		Ttl:       time.Hour,
		MaxSizeMb: 10,
	})
	if err != nil {
		t.Fatalf("failed to create query cache: %v", err)
	}

	indexItem := &IndexItem{
		Key:       "test-key",
		PageCount: pageCount,
	}

	// populate every page so doGet succeeds regardless of ctx state (the
	// in-memory cache store does not consult ctx)
	for i := 0; i < pageCount; i++ {
		pageKey := getPageKey(indexItem.Key, i)
		if err := doSet[*sdkproto.QueryResult](context.Background(), pageKey, &sdkproto.QueryResult{}, time.Hour, queryCache.cache, nil); err != nil {
			t.Fatalf("failed to seed cache page %d: %v", i, err)
		}
	}

	req := &CacheRequest{CallId: "test-call"}
	subscriber := newCacheResultSubscriber(queryCache, indexItem, req, func(*sdkproto.Row) {})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// this must not panic; the return value is not the point of this test
	_ = subscriber.waitUntilDone(ctx)
}

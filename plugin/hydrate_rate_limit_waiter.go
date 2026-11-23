package plugin

import (
	"context"
	"sync"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/rate_limiter"
)

// WaitForHydrateRateLimit waits for the rate limiters of the hydrate call that ctx belongs to,
// in a plugin that sets [Plugin.ClientWaitsForHydrateRateLimit]. The plugin's API client calls it
// before each request it sends. The first call in a hydrate call takes the call's token and waits for it;
// a call made while that wait runs returns when it ends, and a later call returns at once.
// A wait whose ctx is cancelled does not count as the call's token, so the next call waits again.
// Anywhere else (a list or get call, or a plugin that does not set the field) it returns at once.
func WaitForHydrateRateLimit(ctx context.Context) {
	if w, ok := ctx.Value(hydrateRateLimitWaiterKey{}).(*hydrateRateLimitWaiter); ok {
		w.wait(ctx)
	}
}

type hydrateRateLimitWaiterKey struct{}

// hydrateRateLimitWaiter holds the rate limiters of one hydrate call, in a plugin whose API client waits for them.
// It reaches the client through the hydrate function's context.
type hydrateRateLimitWaiter struct {
	limiter          *rate_limiter.MultiLimiter
	metadata         *hydrateMetadata
	concurrencyDelay time.Duration

	// held for the whole wait, so a wait made at the same time returns when the first one ends
	mut sync.Mutex
	// whether the call has its token
	done bool
}

func newHydrateRateLimitWaiter(limiter *rate_limiter.MultiLimiter, metadata *hydrateMetadata, concurrencyDelay time.Duration) *hydrateRateLimitWaiter {
	return &hydrateRateLimitWaiter{limiter: limiter, metadata: metadata, concurrencyDelay: concurrencyDelay}
}

func (w *hydrateRateLimitWaiter) wait(ctx context.Context) {
	// a request with a cancelled ctx is not sent, so it needs no token
	if ctx.Err() != nil {
		return
	}
	w.mut.Lock()
	defer w.mut.Unlock()
	if w.done || ctx.Err() != nil {
		return
	}
	rateLimitDelay := w.limiter.Wait(ctx)
	// the limiter cancels a cancelled wait's reservation, so the call does not have its token yet
	if ctx.Err() != nil {
		return
	}
	w.done = true
	w.metadata.DelayMs = (rateLimitDelay + w.concurrencyDelay).Milliseconds()
}

func contextWithHydrateRateLimitWaiter(ctx context.Context, w *hydrateRateLimitWaiter) context.Context {
	return context.WithValue(ctx, hydrateRateLimitWaiterKey{}, w)
}

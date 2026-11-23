package plugin

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v6/rate_limiter"
	"sync/atomic"
	"time"
)

// a struct defining the rate limiting config the for fetch (list/get) call
type fetchCallRateLimiters struct {
	// rate limiter for the get/single-level-list/parent-list call
	rateLimiter *rate_limiter.MultiLimiter

	// rate limiters for the child list call - populated if this is a list call and the list has a parent hydrate
	childListRateLimiter *rate_limiter.MultiLimiter

	// set once the SDK has waited before calling the list hydrate function (waitForListCall), or before
	// calling a child list function (forChildListCall). That wait pays for the call's first page,
	// so the first WaitForListRateLimit call from the list function does not wait again (see waitForListPage),
	// unless the list function has streamed an item since (see itemStreamed)
	listCallPaid atomic.Bool
}

// if there is a fetch call rate limiter, wait for it
func (l *fetchCallRateLimiters) wait(ctx context.Context) time.Duration {
	if l.rateLimiter != nil {
		return l.rateLimiter.Wait(ctx)
	}
	return 0
}

// if there is a 'childList' rate limiter, wait for it
func (l *fetchCallRateLimiters) childListWait(ctx context.Context) time.Duration {
	if l.childListRateLimiter != nil {
		return l.childListRateLimiter.Wait(ctx)
	}
	return 0
}

// waitForListCall waits for the fetch call rate limiter before the list hydrate function is called.
// The token it takes pays for the list call's first page: if the list function calls WaitForListRateLimit
// before each page, the first of those calls does not take a second token (see waitForListPage).
// A list function which never calls WaitForListRateLimit has used one token, as it always did
func (l *fetchCallRateLimiters) waitForListCall(ctx context.Context) time.Duration {
	delay := l.wait(ctx)
	l.listCallPaid.Store(true)
	return delay
}

// waitForListPage is called by the list hydrate function (via QueryData.WaitForListRateLimit) before each page.
// The first call is already paid for by waitForListCall (or forChildListCall), so it returns at once,
// unless the list function has streamed an item before it: then the SDK's token paid for the page those items
// came from, and this call is for a later page, so it waits (see itemStreamed). Every later call waits.
// (If the list hydrate function is retried, the retry's first call waits, as the first attempt has used the token.)
func (l *fetchCallRateLimiters) waitForListPage(ctx context.Context) time.Duration {
	if l.listCallPaid.CompareAndSwap(true, false) {
		return 0
	}
	return l.wait(ctx)
}

// forChildListCall returns the rate limiters for one child list call of a parent-child list.
// The SDK has waited for the child list rate limiter before the child list function is called, which pays for
// the child's first page, so the first WaitForListRateLimit call from the child does not wait again.
// Each child call has its own copy, so one child's first page never uses up the free page of another call
func (l *fetchCallRateLimiters) forChildListCall() *fetchCallRateLimiters {
	res := &fetchCallRateLimiters{
		rateLimiter:          l.rateLimiter,
		childListRateLimiter: l.childListRateLimiter,
	}
	res.listCallPaid.Store(true)
	return res
}

// itemStreamed is called each time the list function streams items. The page those items came from was paid for,
// either by the SDK's wait before the list function (for the first page) or by a WaitForListRateLimit call,
// so a WaitForListRateLimit call after it is for a further page and must wait.
// This covers a list function whose first page comes with no wait and which waits only before each later page,
// such as the Azure plugin's `for result.NotDone() { d.WaitForListRateLimit(ctx); result.NextWithContext(ctx) }` loops
func (l *fetchCallRateLimiters) itemStreamed() {
	if l == nil {
		return
	}
	// read first, so streaming each item does not write the flag once it is clear
	if l.listCallPaid.Load() {
		l.listCallPaid.Store(false)
	}
}

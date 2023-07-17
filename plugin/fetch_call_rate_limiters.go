package plugin

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v5/rate_limiter"
)

// a struct defining the rate limiting config the for fetch (list/get) call
type fetchCallRateLimiters struct {
	// rate limiter for the get/single-level-list/parent-list call
	rateLimiter *rate_limiter.MultiLimiter
	cost        int

	// rate limiters for the child list call - populated if this is a list call and the list has a parent hydrate
	childListRateLimiter *rate_limiter.MultiLimiter
	childListCost        int
}

// if there is a fetch call rate limiter, wait for it
func (l fetchCallRateLimiters) wait(ctx context.Context) {
	if l.rateLimiter != nil {
		l.rateLimiter.Wait(ctx, l.cost)
	}
}

// if there is a 'childList' rate limiter, wait for it
func (l fetchCallRateLimiters) childListWait(ctx context.Context) {
	if l.childListRateLimiter != nil {
		l.childListRateLimiter.Wait(ctx, l.childListCost)
	}
}

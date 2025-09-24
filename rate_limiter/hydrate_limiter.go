package rate_limiter

import (
	"fmt"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
	"log"
	"strings"
)

type HydrateLimiter struct {
	Name        string
	scopeValues map[string]string
	// underlying rate limiter
	limiter *rate.Limiter
	// semaphore to control concurrency
	sem            *semaphore.Weighted
	maxConcurrency int64
}

func newLimiter(l *Definition, scopeValues map[string]string) *HydrateLimiter {
	log.Printf("[INFO] newLimiter, defintion: %v, scopeValues %v", l, scopeValues)

	res := &HydrateLimiter{
		Name:           l.Name,
		scopeValues:    scopeValues,
		maxConcurrency: l.MaxConcurrency,
	}
	if l.FillRate != 0 {
		res.limiter = rate.NewLimiter(l.FillRate, int(l.BucketSize))
	}
	if l.MaxConcurrency != 0 {
		res.sem = semaphore.NewWeighted(l.MaxConcurrency)
	}
	return res
}
func (l *HydrateLimiter) String() string {
	limiterString := ""
	concurrencyString := ""
	if l.limiter != nil {
		limiterString = fmt.Sprintf("Limit(/s): %v, Burst: %d", l.limiter.Limit(), l.limiter.Burst())
	}
	if l.maxConcurrency >= 0 {
		concurrencyString = fmt.Sprintf("MaxConcurrency: %d", l.maxConcurrency)
	}
	return fmt.Sprintf("%s ScopeValues: %s", strings.Join([]string{limiterString, concurrencyString}, " "), l.scopeValues)
}

func (l *HydrateLimiter) tryToAcquireSemaphore() bool {
	if l.sem == nil {
		return true
	}
	return l.sem.TryAcquire(1)
}

func (l *HydrateLimiter) releaseSemaphore() {
	if l.sem == nil {
		return
	}
	l.sem.Release(1)

}

func (l *HydrateLimiter) reserve() *rate.Reservation {
	if l.limiter != nil {
		return l.limiter.Reserve()
	}
	return nil
}

func (l *HydrateLimiter) hasLimiter() bool {
	return l.limiter != nil
}

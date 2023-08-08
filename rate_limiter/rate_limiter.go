package rate_limiter

import (
	"context"
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
	"log"
	"strings"
	"time"
)

type Limiter struct {
	*rate.Limiter
	Name        string
	scopeValues map[string]string
	sem         *semaphore.Weighted
}

func newLimiter(l *Definition, scopeValues map[string]string) *Limiter {
	return &Limiter{
		Limiter:     rate.NewLimiter(l.FillRate, int(l.BucketSize)),
		Name:        l.Name,
		sem:         semaphore.NewWeighted(l.MaxConcurrency),
		scopeValues: scopeValues,
	}
}

func (l *Limiter) tryToAcquireSemaphore() bool {
	if l.sem == nil {
		return true
	}
	return l.sem.TryAcquire(1)
}

func (l *Limiter) releaseSemaphore() {
	if l.sem == nil {
		return
	}
	l.sem.Release(1)

}

type MultiLimiter struct {
	Limiters    []*Limiter
	ScopeValues map[string]string
}

func NewMultiLimiter(limiters []*Limiter, scopeValues map[string]string) *MultiLimiter {
	res := &MultiLimiter{
		Limiters:    limiters,
		ScopeValues: scopeValues,
	}

	return res
}

func (m *MultiLimiter) Wait(ctx context.Context, cost int) time.Duration {
	// short circuit if we have no limiters
	if len(m.Limiters) == 0 {
		return 0
	}

	var maxDelay time.Duration
	var reservations []*rate.Reservation

	// todo cancel reservations for all but longest delay
	// todo think about burst rate

	// find the max delay from all the limiters
	for _, l := range m.Limiters {
		r := l.ReserveN(time.Now(), cost)
		reservations = append(reservations, r)
		if d := r.Delay(); d > maxDelay {
			maxDelay = d
		}
	}
	if maxDelay == 0 {
		return 0
	}

	log.Printf("[INFO] rate limiter waiting %dms", maxDelay.Milliseconds())
	// wait for the max delay time
	t := time.NewTimer(maxDelay)
	defer t.Stop()
	select {
	case <-t.C:
		// We can proceed.
	case <-ctx.Done():
		// Context was canceled before we could proceed.  Cancel the
		// reservations, which may permit other events to proceed sooner.
		for _, r := range reservations {
			r.Cancel()
		}
	}
	return maxDelay
}

func (m *MultiLimiter) String() string {
	var strs []string

	for _, l := range m.Limiters {
		strs = append(strs, fmt.Sprintf("Name: %sm Limit: %d, Burst: %d, Tags: %s", l.Name, int(l.Limiter.Limit()), l.Limiter.Burst(), l.scopeValues))
	}
	return strings.Join(strs, "\n")
}

func (m *MultiLimiter) LimiterNames() []string {
	var names = make([]string, len(m.Limiters))
	for i, l := range m.Limiters {
		names[i] = l.Name
	}
	return names
}

func (m *MultiLimiter) TryToAcquireSemaphore() bool {

	// keep track of limiters whose semaphore we have acquired
	var acquired []*Limiter
	for _, l := range m.Limiters {

		if l.tryToAcquireSemaphore() {
			acquired = append(acquired, l)

		} else {

			// we failed to acquire the semaphore -
			// we must release all acquired semaphores
			for _, a := range acquired {
				a.releaseSemaphore()
			}
			return false
		}
	}

	return true
}

func (m *MultiLimiter) ReleaseSemaphore() {
	for _, l := range m.Limiters {
		l.releaseSemaphore()
	}
}

// FormatStringMap orders the map keys and returns a string containing all map keys and values
func FormatStringMap(stringMap map[string]string) string {
	var strs []string

	for _, k := range helpers.SortedMapKeys(stringMap) {
		strs = append(strs, fmt.Sprintf("%s=%s", k, stringMap[k]))
	}

	return strings.Join(strs, ",")
}

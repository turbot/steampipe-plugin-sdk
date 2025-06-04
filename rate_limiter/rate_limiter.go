package rate_limiter

import (
	"context"
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"golang.org/x/time/rate"
	"log"
	"strings"
	"time"
)

type MultiLimiter struct {
	Limiters    []*HydrateLimiter
	ScopeValues map[string]string
}

func NewMultiLimiter(limiters []*HydrateLimiter, scopeValues map[string]string) *MultiLimiter {
	res := &MultiLimiter{
		Limiters:    limiters,
		ScopeValues: scopeValues,
	}

	return res
}

func EmptyMultiLimiter() *MultiLimiter {
	return &MultiLimiter{ScopeValues: make(map[string]string)}

}

func (m *MultiLimiter) Wait(ctx context.Context) time.Duration {
	// short circuit if we have no limiters
	if len(m.Limiters) == 0 {
		log.Printf("[DEBUG] MultiLimiter.Wait() no limiters, returning immediately")
		return 0
	}

	var maxDelay time.Duration = 0
	var reservations []*rate.Reservation

	// todo cancel reservations for all but longest delay
	// todo think about burst rate

	// find the max delay from all the limiters
	for _, l := range m.Limiters {
		if l.hasLimiter() {
			r := l.reserve()
			reservations = append(reservations, r)
			d := r.Delay()
			if d > maxDelay {
				maxDelay = d
			}
		}
	}

	if maxDelay == 0 {
		return 0
	}

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
		strs = append(strs, l.String())
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
	var acquired []*HydrateLimiter
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

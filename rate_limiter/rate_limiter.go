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

type Limiter struct {
	*rate.Limiter
	scopeValues *ScopeValues
}

type MultiLimiter struct {
	Limiters []*Limiter
}

func (m *MultiLimiter) Wait(ctx context.Context, cost int) {
	// short circuit if we have no limiters
	if len(m.Limiters) == 0 {
		return
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
		return
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
}

func (m *MultiLimiter) String() string {
	var strs []string

	for _, l := range m.Limiters {
		strs = append(strs, fmt.Sprintf("Limit: %d, Burst: %d, Tags: %s", int(l.Limiter.Limit()), l.Limiter.Burst(), l.scopeValues))
	}
	return strings.Join(strs, "\n")
}

// FormatStringMap orders the map keys and returns a string containing all map keys and values
func FormatStringMap(stringMap map[string]string) string {
	var strs []string

	for _, k := range helpers.SortedMapKeys(stringMap) {
		strs = append(strs, fmt.Sprintf("%s=%s", k, stringMap[k]))
	}

	return strings.Join(strs, ",")
}

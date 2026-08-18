package plugin

import (
	"context"
	"math"
	"sync/atomic"
)

type queryStatus struct {
	rowsRequired int64
	// counters mutated and read concurrently: the streaming goroutine stamps
	// running totals onto each row's metadata (QueryData.streamRow) while the
	// list goroutine and the per-row hydrate goroutines increment them. They are
	// atomic.Int64 so those accesses never race (a plain field is trivially
	// read non-atomically by mistake; the type makes that a compile error).
	rowsStreamed      atomic.Int64
	hydrateCalls      atomic.Int64
	cachedRowsFetched atomic.Int64
	// flag which is true when we have streamed enough rows (or the context is cancelled)
	StreamingComplete bool
}

func newQueryStatus(limit *int64) *queryStatus {
	var rowsRequired int64 = math.MaxInt32
	if limit != nil {
		rowsRequired = *limit
	}
	return &queryStatus{
		rowsRequired: rowsRequired,
	}
}

// RowsRemaining returns how many rows are required to complete the query
//   - if no limit has been parsed from the query, this will return math.MaxInt32
//     (meaning an unknown number of rows remain)
//   - if there is a limit, it will return the number of rows required to reach this limit
//   - if  the context has been cancelled, it will return zero
func (s *queryStatus) RowsRemaining(ctx context.Context) int64 {
	if IsCancelled(ctx) {
		return 0
	}
	rowsRemaining := s.rowsRequired - s.rowsStreamed.Load()
	return rowsRemaining
}

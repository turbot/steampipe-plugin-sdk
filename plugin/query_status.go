package plugin

import (
	"context"
	"math"
)

type queryStatus struct {
	rowsRequired      int64
	rowsStreamed      int64
	hydrateCalls      int64
	cachedRowsFetched int64
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
	rowsRemaining := s.rowsRequired - s.rowsStreamed
	return rowsRemaining
}

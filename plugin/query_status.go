package plugin

import (
	"context"
	"math"
)

type QueryStatus struct {
	rowsRequired uint64
	rowsStreamed uint64
	hydrateCalls uint64
	cacheHit     bool
}

func newQueryStatus(limit *int64) *QueryStatus {
	var rowsRequired uint64 = math.MaxUint32
	if limit != nil {
		rowsRequired = uint64(*limit)
	}
	return &QueryStatus{
		rowsRequired: rowsRequired,
	}
}

// RowsRemaining returns how many rows are required to complete the query
// - if no limit has been parsed from the query, this will return math.MaxInt32
//   (meaning an unknown number of rows remain)
// - if there is a limit, it will return the number of rows required to reach this limit
// - if  the context has been cancelled, it will return zero
func (s *QueryStatus) RowsRemaining(ctx context.Context) uint64 {
	if IsCancelled(ctx) {
		return 0
	}
	rowsRemaining := s.rowsRequired - s.rowsStreamed
	return rowsRemaining
}

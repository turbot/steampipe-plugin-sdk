package plugin

import (
	"context"
	"math"
)

type QueryStatus struct {
	rowsRequired int
	rowsStreamed int
	hydrateCalls uint64
}

func newQueryStatus(limit *int64) *QueryStatus {
	var rowsRequired = math.MaxInt32
	if limit != nil {
		rowsRequired = int(*limit)
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
func (s *QueryStatus) RowsRemaining(ctx context.Context) int {
	if IsCancelled(ctx) {
		return 0
	}
	rowsRemaining := s.rowsRequired - s.rowsStreamed
	return rowsRemaining
}

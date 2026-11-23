package plugin

import (
	"context"
	"errors"
	"testing"
)

func newWaitTestRowData() *rowData {
	return &rowData{
		waitChan:  make(chan bool),
		errorChan: make(chan error, 2),
		queryData: &QueryData{},
	}
}

// A failed hydrate call puts its error on errorChan before it counts as finished, so by the time waitChan closes
// the error is already waiting. With both ready, the wait used to pick waitChan half the time and report no error.
// Both are filled before the wait starts, so both cases are always ready when it reaches its select.
func TestWaitForCompletionOrErrorReturnsErrorWhenWaitChanClosedToo(t *testing.T) {
	hydrateErr := errors.New("hydrate call failed: 403")
	for i := 0; i < 100; i++ {
		r := newWaitTestRowData()
		r.errorChan <- hydrateErr
		close(r.waitChan)

		if err := r.waitForCompletionOrError(); !errors.Is(err, hydrateErr) {
			t.Fatalf("run %d: expected %v, got %v", i, hydrateErr, err)
		}
	}
}

// With no error, a closed waitChan is a clean finish.
func TestWaitForCompletionOrErrorReturnsNilWhenNoError(t *testing.T) {
	for i := 0; i < 100; i++ {
		r := newWaitTestRowData()
		close(r.waitChan)

		if err := r.waitForCompletionOrError(); err != nil {
			t.Fatalf("run %d: expected nil, got %v", i, err)
		}
	}
}

// The error path still works when the error arrives while the wait is blocked.
func TestWaitForCompletionOrErrorReturnsErrorWhileWaiting(t *testing.T) {
	hydrateErr := errors.New("hydrate call failed")
	r := newWaitTestRowData()
	go func() { r.errorChan <- hydrateErr }()

	if err := r.waitForCompletionOrError(); !errors.Is(err, hydrateErr) {
		t.Fatalf("expected %v, got %v", hydrateErr, err)
	}
}

// End to end: a row with an error already queued returns the error and no row; one without returns its row.
func TestWaitForHydrateCallsToComplete(t *testing.T) {
	hydrateErr := errors.New("hydrate call failed: 403")

	r := newWaitTestRowData()
	r.errorChan <- hydrateErr
	row, err := r.waitForHydrateCallsToComplete(context.Background())
	if !errors.Is(err, hydrateErr) || row != nil {
		t.Fatalf("expected the hydrate error and no row, got row %v, err %v", row, err)
	}

	r = newWaitTestRowData()
	row, err = r.waitForHydrateCallsToComplete(context.Background())
	if err != nil || row == nil {
		t.Fatalf("expected a row and no error, got row %v, err %v", row, err)
	}
}

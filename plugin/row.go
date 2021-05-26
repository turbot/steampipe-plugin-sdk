package plugin

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/logging"
	"github.com/turbot/steampipe-plugin-sdk/plugin/context_key"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RowData contains the row data
type RowData struct {
	// the output of the get/list call which is passed to all other hydrate calls
	Item interface{}
	// if there was a parent-child list call, store the parent list item
	ParentItem     interface{}
	matrixItem     map[string]interface{}
	hydrateResults map[string]interface{}
	mut            sync.Mutex
	waitChan       chan bool
	wg             sync.WaitGroup
	table          *Table
	errorChan      chan error
	queryData      *QueryData
}

// placeholder struct to return when the hydrate function does not return anything
// - this allows us to determine the hydrate function _was_ called
type emptyHydrateResults struct{}

// newRowData creates an empty rowData object
func newRowData(d *QueryData, item interface{}) *RowData {
	// create buffered error channel for any errors occurring hydrate functions (+2 is for the get and list hydrate calls)
	errorChan := make(chan error, len(d.hydrateCalls)+2)

	return &RowData{
		Item:           item,
		matrixItem:     map[string]interface{}{},
		hydrateResults: map[string]interface{}{},
		waitChan:       make(chan bool),
		table:          d.Table,
		errorChan:      errorChan,
		queryData:      d,
	}
}

func (r *RowData) getRow(ctx context.Context) (*proto.Row, error) {
	// NOTE: the RowData (may) have matrixItem set
	// (this is a data structure containing fetch specific data, e.g. region)
	// store this in the context for use by the transform functions
	rowDataCtx := context.WithValue(ctx, context_key.MatrixItem, r.matrixItem)

	// make any required hydrate function calls
	// - these populate the row with data entries corresponding to the hydrate function nameSP_LOG=TRACE
	// keep looping round hydrate functions until they are all started

	// make a map of started hydrate calls for this row - this is used the determine which calls have not started yet
	var callsStarted = map[string]bool{}

	for {
		var allStarted = true
		for _, call := range r.queryData.hydrateCalls {
			hydrateFuncName := helpers.GetFunctionName(call.Func)
			if !callsStarted[hydrateFuncName] {
				if call.CanStart(r, hydrateFuncName, r.queryData.concurrencyManager) {
					// execute the hydrate call asynchronously
					call.Start(rowDataCtx, r, hydrateFuncName, r.queryData.concurrencyManager)
					callsStarted[hydrateFuncName] = true
				} else {
					allStarted = false
				}
			}
		}
		if allStarted {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	var row *proto.Row

	// wait for all hydrate calls to complete and signal via the wait chan when they are
	// (we need this slightly convoluted mechanism to allow us to check for upstream errors
	// by also selecting the errorChan)
	go func() {
		r.wg.Wait()
		logging.LogTime("all hydrate calls complete")
		var err error

		// now execute any transforms required to populate the column values
		row, err = r.getColumnValues(rowDataCtx)
		if err != nil {
			r.queryData.streamError(err)
		}
		close(r.waitChan)
	}()

	// select both wait chan and error chan
	select {
	case err := <-r.errorChan:
		log.Println("[WARN] hydrate err chan select", "error", err)
		return nil, err
	case <-r.waitChan:
		logging.LogTime("send a row")
		return row, nil
	}
}

// generate the column values for for all requested columns
func (r *RowData) getColumnValues(ctx context.Context) (*proto.Row, error) {
	row := &proto.Row{Columns: make(map[string]*proto.Column)}
	// only populate columns which have been asked for
	for _, columnName := range r.queryData.QueryContext.Columns {
		// get columns schema
		column := r.table.getColumn(columnName)
		if column == nil {
			// postgres asked for a non existent column. Shouldn't happen but just ignore
			continue
		}

		var err error
		row.Columns[columnName], err = r.table.getColumnValue(ctx, r, column)
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}

// invoke a hydrate function, and set results on the rowData object. Stream errors on the rowData error channel
func (r *RowData) callHydrate(ctx context.Context, d *QueryData, hydrateFunc HydrateFunc, hydrateKey string, retryConfig *RetryConfig, shouldIgnoreError ErrorPredicate) {
	// handle panics in the row hydrate function
	defer func() {
		if p := recover(); p != nil {
			r.errorChan <- status.Error(codes.Internal, fmt.Sprintf("hydrate call %s failed with panic %v", hydrateKey, p))
		}
		r.wg.Done()
	}()

	logging.LogTime(hydrateKey + " start")

	// now call the hydrate function, passing the item and hydrate results so far
	hydrateData, err := r.callHydrateWithRetries(ctx, d, hydrateFunc, retryConfig, shouldIgnoreError)
	if err != nil {
		log.Printf("[ERROR] callHydrate %s finished with error: %v\n", hydrateKey, err)
		r.errorChan <- err
	} else if hydrateData != nil {
		r.set(hydrateKey, hydrateData)
	} else {
		// the the hydrate results to an empty data object
		r.set(hydrateKey, emptyHydrateResults{})
	}

	logging.LogTime(hydrateKey + " end")
}

// invoke a hydrate function, retrying as required based on the retry config, and return the result and/or error
func (r *RowData) callHydrateWithRetries(ctx context.Context, d *QueryData, hydrateFunc HydrateFunc, retryConfig *RetryConfig, shouldIgnoreError ErrorPredicate) (interface{}, error) {
	hydrateData := &HydrateData{Item: r.Item, ParentItem: r.ParentItem, HydrateResults: r.hydrateResults}
	// WrapHydrate function returns a HydrateFunc which handles Ignorable errors
	hydrateWithIgnoreError := WrapHydrate(hydrateFunc, shouldIgnoreError)
	hydrateResult, err := hydrateWithIgnoreError(ctx, d, hydrateData)
	if err != nil {
		if shouldRetryError(err, d, retryConfig) {
			hydrateData := &HydrateData{Item: r.Item, ParentItem: r.ParentItem, HydrateResults: r.hydrateResults}
			hydrateResult, err = RetryHydrate(ctx, d, hydrateData, hydrateFunc, retryConfig)
		}
	}
	return hydrateResult, err
}

func shouldRetryError(err error, d *QueryData, retryConfig *RetryConfig) bool {
	if retryConfig == nil {
		return false
	}
	// if we have started streaming, we cannot retry
	if d.streamCount != 0 {
		return false
	}
	shouldRetryErrorFunc := retryConfig.ShouldRetryError
	if shouldRetryErrorFunc == nil {
		return false
	}
	// a shouldRetryErrorFunc is declared in the retry config - call it to see if we should retry
	return shouldRetryErrorFunc(err)
}

func (r *RowData) set(key string, item interface{}) error {
	r.mut.Lock()
	defer r.mut.Unlock()
	if _, ok := r.hydrateResults[key]; ok {
		return fmt.Errorf("failed to save item - row data already contains item for key %s", key)
	}
	r.hydrateResults[key] = item

	return nil
}

// get the name of the hydrate function which have completed
func (r *RowData) getHydrateKeys() []string {
	r.mut.Lock()
	defer r.mut.Unlock()
	var keys []string
	for key := range r.hydrateResults {
		keys = append(keys, key)
	}
	return keys
}

// GetColumnData returns the root item, and, if this column has a hydrate function registered, the associated hydrate data
func (r *RowData) GetColumnData(column *Column) (interface{}, error) {

	if column.resolvedHydrateName == "" {
		return nil, fmt.Errorf("column %s has no resolved hydrate function name", column.Name)
	}

	if hydrateItem, ok := r.hydrateResults[column.resolvedHydrateName]; !ok {
		log.Printf("[ERROR] table '%s' column '%s' requires hydrate data from %s but none is available.\n", r.table.Name, column.Name, column.resolvedHydrateName)
		return nil, fmt.Errorf("column '%s' requires hydrate data from %s but none is available", column.Name, column.resolvedHydrateName)
	} else {
		return hydrateItem, nil
	}
}
